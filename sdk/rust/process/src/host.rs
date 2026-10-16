use super::{
    backend::Process,
    callbacks::{Frame, Registry},
    channel::Channel,
    *,
};
use cordis_sdk_core::Context;
use cordis_wire::{Identity, Incoming, Request, Response};
use futures_util::FutureExt;
use tokio::sync::{Notify, mpsc, watch};

#[cfg(test)]
#[path = "resource_tests.rs"]
mod resource_tests;

pub struct Host<P> {
    descriptor: cordis_sdk_core::Descriptor,
    pub channel: Rc<Channel>,
    pub bindings: Rc<cordis_sdk_core::Bindings>,
    plugin: Rc<P>,
    dependencies: Rc<BTreeMap<String, String>>,
    registry: Rc<Registry>,
    resources: super::resources::Resources,
    active: Cell<bool>,
    attempted: Cell<bool>,
    closing: Cell<bool>,
    seen: RefCell<BTreeSet<String>>,
    inflight: Cell<usize>,
    idle: Notify,
    lanes: [Cell<usize>; 9],
    pub url: Option<String>,
    pub done: watch::Sender<Option<Result<()>>>,
}
fn decimal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 20
        && (value == "0" || !value.starts_with('0'))
        && value.parse::<u64>().is_ok()
}
fn identity(value: &Identity) -> bool {
    decimal(&value.incarnation) && decimal(&value.fiber) && decimal(&value.epoch)
}
impl<P: Plugin> Host<P> {
    fn hello(&self, request: &Request) -> Result<Value> {
        if self.channel.identity.borrow().is_some() {
            return Err(refused("protocol", "Hello must be first"));
        }
        let manifest = P::manifest();
        let args = &request.payload.control;
        let validation = serde_json::from_value(args["validation"].clone())
            .map_err(|e| refused("invalid_argument", format!("validation: {e}")))?;
        let descriptor = &self.descriptor;
        cordis_sdk_core::contract::compare(descriptor, &args["interface"])
            .map_err(|e| refused("contract_mismatch", e))?;
        let dependency_interfaces = P::dependency_interfaces();
        if dependency_interfaces.keys().ne(self.dependencies.keys())
            || args["dependency_interfaces"].as_object().map(|v| v.len())
                != Some(dependency_interfaces.len())
        {
            return Err(refused(
                "contract_mismatch",
                "dependency interface identities differ",
            ));
        }
        for (name, interface) in &dependency_interfaces {
            let expected = cordis_sdk_core::Descriptor::new(interface.clone())
                .map_err(|e| refused("invalid_contract", e.to_string()))?;
            cordis_sdk_core::contract::compare(&expected, &args["dependency_interfaces"][name])
                .map_err(|e| refused("contract_mismatch", format!("dependencies.{name}: {e}")))?;
        }
        if args["contract"]
            != json!({"name":manifest.provides.name,"revision":manifest.provides.revision})
        {
            return Err(refused("contract_mismatch", "exact revision required"));
        }
        if args["dependencies"] != json!(*self.dependencies) {
            return Err(refused(
                "dependency_mismatch",
                "exact dependency bindings required",
            ));
        }
        for (key, declarations) in [
            ("events", &self.bindings.events),
            ("middleware", &self.bindings.middleware),
        ] {
            let expected = args[key]
                .as_object()
                .ok_or_else(|| refused("binding_mismatch", key))?;
            if expected.len() != declarations.len()
                || declarations.iter().any(|(name, c)| {
                    let value = &args[key][name];
                    value["name"] != c.name
                        || value["revision"] != c.revision
                        || !value["realm"].as_str().is_some_and(decimal)
                })
            {
                return Err(refused("binding_mismatch", key));
            }
        }
        let provisions: BTreeMap<_, _> = self
            .bindings
            .provisions
            .iter()
            .map(|(name, c)| (name, json!({"name":c.name,"revision":c.revision})))
            .collect();
        let mut factories = self.bindings.factories.clone();
        factories.sort();
        let composition = json!({"factories":factories,"provisions":provisions});
        if args["composition"] != composition {
            return Err(refused("composition_mismatch", "exact bindings required"));
        }
        self.channel
            .dependency_resources
            .set(args["capabilities"]["dependency_resources"] == true);
        self.channel.validation.set(validation);
        *self.channel.identity.borrow_mut() =
            Some((request.session.clone(), request.activation.clone()));
        Ok(
            json!({"contract":args["contract"],"interface":cordis_sdk_core::contract::encode(descriptor),"dependency_interfaces":dependency_interfaces,"dependencies":*self.dependencies,"events":args["events"],"middleware":args["middleware"],"composition":composition,
            "http":self.url,"capabilities":{"multiplex":true,"reverse_calls":true,"effects":true,"effect_sequences":true,"scopes":true,
            "tasks":true,"events":true,"middleware":true,"composition":true,"metadata":true,"streams":false,"resources":true,"dependency_resources":self.channel.dependency_resources.get(),"max_inflight":16,"max_message":cordis_wire::MAX_MESSAGE}}),
        )
    }
    async fn execute(&self, request: &Request) -> Result<Payload> {
        let bodies = usize::from(matches!(
            request.command.as_str(),
            "call"
                | "resource_acquire"
                | "resource_call"
                | "event_call"
                | "middleware_call"
                | "middleware_terminal"
        ));
        if request.payload.messages.len() != bodies {
            return Err(refused(
                "invalid_argument",
                "unexpected business message count",
            ));
        }
        if request.session.len() != 32
            || !request
                .session
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !decimal(&request.operation)
            || !identity(&request.activation)
        {
            return Err(refused("protocol", "invalid identity"));
        }
        if request.command == "hello" {
            if !self.seen.borrow_mut().insert(request.operation.clone()) {
                return Err(refused("duplicate_operation", "Hello operation reused"));
            }
            return self.hello(request).map(Into::into);
        }
        if self.channel.identity.borrow().as_ref()
            != Some(&(request.session.clone(), request.activation.clone()))
        {
            return Err(refused("stale_session", "identity mismatch"));
        }
        if self.seen.borrow().len() >= 65536
            || !self.seen.borrow_mut().insert(request.operation.clone())
        {
            return Err(refused("duplicate_operation", "operation unavailable"));
        }
        let command = request.command.as_str();
        let args = &request.payload.control;
        let cleanup = matches!(
            command,
            "deactivate"
                | "resource_release"
                | "effect_release"
                | "sequence_undo"
                | "sequence_release"
                | "event_release"
                | "middleware_release"
        );
        if command == "activate" {
            if self.attempted.replace(true) {
                return Err(refused("closed", "activation already attempted"));
            }
        } else if !self.attempted.get() {
            return Err(refused("closed", "activation not attempted"));
        }
        if command == "deactivate" {
            self.closing.set(true);
            self.active.set(false);
            while self.inflight.get() > 0 {
                self.idle.notified().await;
            }
            if !self.registry.recovered() || !self.resources.recovered() {
                return Err(refused("owned", "callbacks not recovered"));
            }
        }
        let cancellation = if command == "task_call" {
            Some(
                self.registry
                    .get(text(args, "task")?)?
                    .cancellation
                    .subscribe(),
            )
        } else {
            None
        };
        let frame = Frame::new(cancellation);
        let backend = Process {
            channel: self.channel.clone(),
            registry: self.registry.clone(),
            frame: frame.clone(),
            token: args["context"].as_str().unwrap_or("").into(),
            cleanup,
            dependencies: self.dependencies.clone(),
            bindings: self.bindings.clone(),
        };
        let ctx = Context::from_backend(Rc::new(backend));
        let business = matches!(command, "call" | "resource_acquire" | "resource_call");
        let counted =
            business && self.active.get() && !self.closing.get() && self.inflight.get() < 16;
        if counted {
            self.inflight.set(self.inflight.get() + 1);
        }
        let result = std::panic::AssertUnwindSafe(async {
            if business {
                if !self.active.get() || self.closing.get() {
                    return Err(refused("closed", "instance inactive"));
                }
                if !counted {
                    return Err(refused("overloaded", "call capacity reached"));
                }
            }
            let control = match command {
                "resource_acquire" => {
                    let kind = text(args, "kind")?.to_owned();
                    let owner = text(args, "owner")?;
                    let schema = cordis_sdk_core::contract::resource(&self.descriptor, &kind)
                        .map_err(|e| refused("invalid_argument", e))?
                        .with_validation(self.channel.validation.get());
                    let (id, entry) = self.resources.reserve(owner, &kind)?;
                    match self
                        .plugin
                        .clone()
                        .acquire(
                            ctx,
                            kind,
                            cordis_sdk_core::message::incoming::<P>(
                                schema.args.clone(),
                                request
                                    .payload
                                    .one()
                                    .map_err(|e| refused("invalid_argument", e))?,
                            )
                            .map_err(|e| refused("invalid_argument", e))?,
                        )
                        .await
                        .map_err(|e| e.check_declared(&schema.errors, "factory errors"))
                    {
                        Ok(object) => {
                            *entry.value.borrow_mut() = Some(object);
                            Ok(json!({"resource":id,"owner":owner}))
                        }
                        Err(failure) => {
                            if matches!(failure, Failure::Rejected(_) | Failure::Refused(_)) {
                                self.resources.reject(&id);
                            }
                            Err(failure)
                        }
                    }
                }
                "resource_call" => {
                    let entry = self.resources.get(args)?;
                    let method = text(args, "method")?;
                    let schema = cordis_sdk_core::contract::resource(&self.descriptor, &entry.kind)
                        .map_err(|e| refused("invalid_argument", e))?;
                    let signature = cordis_sdk_core::contract::method(&schema.methods, method)
                        .map_err(|e| refused("invalid_argument", e))?
                        .with_validation(self.channel.validation.get());
                    let value = entry
                        .invoke(
                            ctx,
                            frame.clone(),
                            text(args, "method")?.into(),
                            cordis_sdk_core::message::incoming::<P>(
                                signature.args.clone(),
                                request
                                    .payload
                                    .one()
                                    .map_err(|e| refused("invalid_argument", e))?,
                            )
                            .map_err(|e| refused("invalid_argument", e))?,
                        )
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "resource errors"))?;
                    value.check(&signature.result).map_err(unknown)?;
                    return Ok(Payload::message(
                        value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                    ));
                }
                "resource_release" => {
                    self.resources.get(args)?.close(ctx, frame.clone()).await?;
                    Ok(json!({}))
                }
                "activate" => {
                    self.plugin.clone().activate(ctx).await?;
                    self.active.set(true);
                    Ok(Value::Null)
                }
                "deactivate" => {
                    self.plugin.clone().deactivate(ctx).await?;
                    Ok(Value::Null)
                }
                "call" => {
                    if !self.active.get() || self.closing.get() {
                        return Err(refused("closed", "instance inactive"));
                    }
                    if !counted {
                        return Err(refused("overloaded", "call capacity reached"));
                    }
                    let method = text(args, "method")?;
                    let signature = cordis_sdk_core::contract::method(
                        &self.descriptor.interface().methods,
                        method,
                    )
                    .map_err(|e| refused("invalid_argument", e))?
                    .with_validation(self.channel.validation.get());
                    let value = self
                        .plugin
                        .clone()
                        .invoke(
                            ctx,
                            method.into(),
                            cordis_sdk_core::message::incoming::<P>(
                                signature.args.clone(),
                                request
                                    .payload
                                    .one()
                                    .map_err(|e| refused("invalid_argument", e))?,
                            )
                            .map_err(|e| refused("invalid_argument", e))?,
                        )
                        .await
                        .map_err(|e| e.check_declared(&signature.errors, "method errors"))?;
                    value.check(&signature.result).map_err(unknown)?;
                    return Ok(Payload::message(
                        value.encode(cordis_wire::MAX_MESSAGE).map_err(unknown)?,
                    ));
                }
                "effect_release" => {
                    self.registry
                        .get(text(args, "effect")?)?
                        .call(ctx, Value::Null, true, true)
                        .await
                }
                "sequence_next" | "sequence_undo" | "sequence_release" => {
                    self.registry
                        .sequence_call(
                            command,
                            text(args, "sequence")?,
                            args["step"].as_str().unwrap_or(""),
                            ctx,
                        )
                        .await
                }
                "scope_call" => {
                    let entry = self.registry.get(text(args, "run")?)?;
                    checked_outcome(entry.call(ctx, Value::Null, true, true).await)
                }
                "task_call" => {
                    let entry = self.registry.get(text(args, "task")?)?;
                    let result = entry.clone().call(ctx, Value::Null, true, true).await;
                    // A task result, including failure, is cached by the Runtime.
                    let removed = entry.callback.borrow_mut().take();
                    drop(removed);
                    checked_outcome(result)
                }
                "task_cancel" => {
                    self.registry
                        .get(text(args, "task")?)?
                        .cancellation
                        .send_replace(true);
                    Ok(Value::Null)
                }
                "event_release" => {
                    self.registry.release(text(args, "listener")?)?;
                    Ok(Value::Null)
                }
                "middleware_release" => {
                    self.registry.release(text(args, "handler")?)?;
                    Ok(Value::Null)
                }
                "event_call" | "middleware_call" | "middleware_terminal" => {
                    self.registry.admit(text(args, "call")?)?;
                    let (key, once) = match command {
                        "event_call" => ("listener", false),
                        "middleware_call" => ("handler", false),
                        _ => ("run", true),
                    };
                    return self
                        .registry
                        .get(text(args, key)?)?
                        .call_payload(ctx, request.payload.clone(), once, once)
                        .await;
                }
                _ => Err(refused("unknown_command", command)),
            };
            control.map(Into::into)
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(unknown("plugin callback panicked")));
        frame.finish().await;
        if counted {
            self.inflight.set(self.inflight.get() - 1);
            self.idle.notify_one();
        }
        result
    }
    pub async fn dispatch(&self, request: Request) -> Result<Response> {
        let lane = match request.command.as_str() {
            "middleware_call" => 7,
            "middleware_terminal" => 8,
            "event_call" => 6,
            "task_call" => 4,
            "task_cancel" => 5,
            "scope_call" => 3,
            "sequence_next" => 2,
            "effect_release" | "sequence_undo" | "sequence_release" | "event_release"
            | "middleware_release" | "resource_release" => 1,
            _ => 0,
        };
        let result = if self.lanes[lane].get() >= 16 {
            Err(refused("overloaded", "callback lane full"))
        } else {
            self.lanes[lane].set(self.lanes[lane].get() + 1);
            let result = self.execute(&request).await;
            self.lanes[lane].set(self.lanes[lane].get() - 1);
            result
        };
        // Session v1 has no explicit unknown-outcome variant. Losing the
        // connection is the only honest way to report uncertain execution.
        if let Err(e @ Failure::Unknown(_)) = &result {
            self.done.send_replace(Some(Err(e.clone())));
            return Err(e.clone());
        }
        Ok(request.response(outcome(result)))
    }
}
pub async fn serve<P: Plugin>(options: Options) -> Result<()> {
    let (send, mut output) = mpsc::channel(256);
    let channel = Channel::new(send);
    let listener = if options.http {
        Some(
            tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .map_err(unknown)?,
        )
    } else {
        None
    };
    let url = listener
        .as_ref()
        .map(|l| l.local_addr().map(|a| format!("http://{a}/cordis/call")))
        .transpose()
        .map_err(unknown)?;
    let manifest = P::manifest();
    let dependencies: BTreeMap<_, _> = manifest
        .requires
        .iter()
        .map(|c| (c.name.clone(), c.revision.clone()))
        .collect();
    if dependencies.len() != manifest.requires.len() {
        return Err(refused("invalid_manifest", "duplicate dependency"));
    }
    let (done, mut stopping) = watch::channel(None);
    let host = Rc::new(Host {
        descriptor: cordis_sdk_core::Descriptor::new(P::interface())
            .map_err(|e| refused("invalid_contract", e.to_string()))?,
        channel: channel.clone(),
        bindings: Rc::new(P::bindings()),
        plugin: Rc::new(P::create(options.config)?),
        dependencies: Rc::new(dependencies),
        registry: Rc::new(Registry::default()),
        resources: super::resources::Resources::default(),
        active: Cell::new(false),
        attempted: Cell::new(false),
        closing: Cell::new(false),
        seen: RefCell::default(),
        inflight: Cell::new(0),
        idle: Notify::new(),
        lanes: std::array::from_fn(|_| Cell::new(0)),
        url,
        done,
    });
    let (flush, mut flushing) = tokio::sync::oneshot::channel::<()>();
    let writer = tokio::task::spawn_local({
        let done = host.done.clone();
        async move {
            // Keep one buffer for the connection. write() flushes every frame,
            // including the final deactivate reply, before reporting success.
            let mut stdout =
                tokio::io::BufWriter::with_capacity(stdio::BUFFER_CAPACITY, tokio::io::stdout());
            let mut draining = false;
            loop {
                let frame = tokio::select! {
                    _ = &mut flushing, if !draining => { output.close(); draining = true; continue; }
                    frame = output.recv() => frame,
                };
                let Some(bytes) = frame else {
                    break;
                };
                if let Err(e) = stdio::write(&mut stdout, &bytes).await {
                    done.send_replace(Some(Err(e.clone())));
                    return Err(e);
                }
            }
            Ok(())
        }
    });
    let http = listener
        .map(|listener| tokio::task::spawn_local(super::http::serve(listener, host.clone())));
    // Header/tag/body reads share a fill instead of scheduling a blocking stdin
    // job for each field. Retain prefetched bytes across all frame reads.
    let mut input = tokio::io::BufReader::with_capacity(stdio::BUFFER_CAPACITY, tokio::io::stdin());
    let mut jobs = tokio::task::JoinSet::new();
    let result = loop {
        let received = tokio::select! { biased; _=stopping.changed()=>{break stopping.borrow().clone().unwrap_or_else(||Err(unknown("host stopped")));}, value=stdio::read(&mut input, host.channel.carrier_validation())=>value };
        let request = match received {
            Ok(Incoming::Request(request)) => request,
            Ok(Incoming::Response(response)) => {
                if let Err(e) = channel.receive(response) {
                    break Err(e);
                }
                continue;
            }
            Err(e) => break Err(e),
        };
        if request.command == "hello" {
            let reply = host.dispatch(request).await?;
            channel.write(reply).await?;
            continue;
        }
        let host = host.clone();
        jobs.spawn_local(async move {
            let deactivate = request.command == "deactivate";
            match host.dispatch(request).await {
                Ok(response) => {
                    let success = response.is_success();
                    if let Err(e) = host.channel.write(response).await {
                        host.done.send_replace(Some(Err(e)));
                    }
                    if deactivate {
                        host.done.send_replace(Some(if success {
                            Ok(())
                        } else {
                            Err(refused("deactivation_failed", "cleanup failed"))
                        }));
                    }
                }
                Err(e) => {
                    host.done.send_replace(Some(Err(e)));
                }
            }
        });
        while jobs.try_join_next().is_some() {}
    };
    channel.close();
    if let Some(http) = http {
        http.abort();
        let _ = http.await;
    }
    if result.is_ok() {
        while jobs.join_next().await.is_some() {}
        // Drain the bounded writer before returning from the process entry.
        let _ = flush.send(());
        writer.await.map_err(unknown)??;
    } else {
        jobs.abort_all();
        writer.abort();
    }
    result
}
