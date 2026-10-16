use crate::{Error, Result, error::outcome, frame};
use cordis_runtime::Context;
use cordis_wire::{Bytes, Identity, Incoming, Payload, Request, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    process::Stdio,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    process::Command,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, timeout},
};

const CAPACITY: usize = 16;
fn lane(command: &str) -> u8 {
    match command {
        "effect_release" | "sequence_undo" | "sequence_release" | "event_release"
        | "middleware_release" | "resource_release" => 1,
        "sequence_next" => 2,
        "scope_call" => 3,
        "task_call" => 4,
        "task_cancel" => 5,
        "event_call" => 6,
        "middleware_call" => 7,
        "middleware_terminal" => 8,
        _ => 0,
    }
}
type Reply = oneshot::Sender<Result<Payload>>;
enum CommandMessage {
    Request(Request, Bytes, Reply),
    Abort(Error),
    StopUnactivated,
}
struct Pending {
    session: String,
    activation: cordis_wire::Identity,
    command: String,
    reply: Reply,
    deadline: Option<Instant>,
}

#[derive(Clone)]
pub(crate) struct Session(Arc<Client>);
#[derive(Clone)]
pub(crate) struct WeakSession(Weak<Client>);
impl WeakSession {
    pub fn upgrade(&self) -> Option<Session> {
        self.0.upgrade().map(Session)
    }
}
struct Client {
    _code: crate::process::CodeLease,
    send: mpsc::Sender<CommandMessage>,
    closed: watch::Receiver<Option<Result<()>>>,
    peer_stopped: watch::Receiver<bool>,
    session: String,
    activation: Identity,
    next: AtomicU64,
    activated: AtomicBool,
    timeout: Duration,
    task_stop_timeout: Duration,
    router: Arc<crate::dependency::Router>,
}

impl Session {
    pub async fn spawn(
        spec: &crate::ProcessSpec,
        activation: Identity,
        router: Arc<crate::dependency::Router>,
        code: crate::process::CodeLease,
    ) -> Result<Self> {
        let deadline = spec.timeout;
        let task_stop_timeout = spec.task_stop_timeout;
        if deadline.is_zero() || Instant::now().checked_add(deadline).is_none() {
            return Err(Error::Protocol(
                "call timeout must be positive and representable".into(),
            ));
        }
        if task_stop_timeout.is_zero() || Instant::now().checked_add(task_stop_timeout).is_none() {
            return Err(Error::InvalidInput(
                "task stop timeout must be positive and representable".into(),
            ));
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| Error::Io(e.to_string()))?;
        let session: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let mut child = Command::new(&spec.executable)
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        // Connection-owned buffers coalesce frame fields without delaying a
        // completed frame: frame::write still flushes before returning.
        let mut stdin = tokio::io::BufWriter::with_capacity(
            frame::BUFFER_CAPACITY,
            child.stdin.take().unwrap(),
        );
        let mut stdout = tokio::io::BufReader::with_capacity(
            frame::BUFFER_CAPACITY,
            child.stdout.take().unwrap(),
        );
        let (send, mut commands) = mpsc::channel(CAPACITY);
        let (closed_tx, closed) = watch::channel(None);
        let (peer_stopped_tx, peer_stopped) = watch::channel(false);
        let (writes, mut outgoing) = mpsc::channel::<Bytes>(CAPACITY * 10);
        let (reads_tx, mut reads) = mpsc::channel(CAPACITY);
        let read_tx = reads_tx.clone();
        let validation = spec.validation;
        let reader = tokio::spawn(async move {
            loop {
                let result = frame::read(&mut stdout, validation).await;
                let failed = result.is_err();
                if read_tx.send(result).await.is_err() || failed {
                    break;
                }
            }
        });
        let writer = tokio::spawn(async move {
            while let Some(bytes) = outgoing.recv().await {
                if let Err(error) = frame::write(&mut stdin, &bytes).await {
                    let _ = reads_tx.send(Err(error)).await;
                    break;
                }
            }
        });
        let callback_router = router.clone();
        let callback_session = session.clone();
        let callback_activation = activation.clone();
        let worker_code = code.clone();
        tokio::spawn(async move {
            let _code = worker_code;
            let mut pending: BTreeMap<String, Pending> = BTreeMap::new();
            let mut callbacks = JoinSet::new();
            let mut disposals = JoinSet::new();
            let mut sequence_waits = JoinSet::new();
            let mut scope_runs = JoinSet::new();
            let mut task_joins = JoinSet::new();
            let mut cleanup_calls = JoinSet::new();
            let mut event_emits = JoinSet::new();
            let mut event_joins = JoinSet::new();
            let mut middleware_runs = JoinSet::new();
            let mut middleware_nexts = JoinSet::new();
            let mut composition_calls = JoinSet::new();
            let mut child_waits = JoinSet::new();
            let mut child_disposals = JoinSet::new();
            let mut resource_calls = JoinSet::new();
            let mut callback_history = BTreeSet::new();
            let mut deactivated = false;
            let mut stopping = None;
            let mut tick = tokio::time::interval(Duration::from_millis(25));
            let termination = loop {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some(CommandMessage::Request(request, bytes, reply)) => {
                            if pending.values().filter(|p| lane(&p.command) == lane(&request.command)).count() >= CAPACITY { let _ = reply.send(Err(Error::Overloaded)); continue; }
                            if stopping.is_some() { let _ = reply.send(Err(Error::Closed)); continue; }
                            let request_timeout = if request.command == "task_cancel" { task_stop_timeout } else if matches!(request.command.as_str(), "hello" | "activate" | "deactivate") { deadline.max(Duration::from_secs(5)) } else { deadline };
                            if request.command == "deactivate" { stopping = Some(Instant::now() + request_timeout); }
                            if writes.try_send(bytes).is_err() { let _ = reply.send(Err(Error::Overloaded)); continue; }
                            let expires = if request.command == "task_call" { None } else { Some(Instant::now() + request_timeout) };
                            pending.insert(request.operation.clone(), Pending { session: request.session, activation: request.activation, command: request.command, reply, deadline: expires });
                        }
                        Some(CommandMessage::Abort(error)) => break Err(error),
                        Some(CommandMessage::StopUnactivated) => break Ok(()),
                        None => break Err(Error::Closed),
                    },
                    received = reads.recv() => match received {
                        Some(Ok(incoming)) => {
                            if let Incoming::Request(request) = incoming {
                                if request.session != callback_session || request.activation != callback_activation
                                    || !matches!(request.command.as_str(), "dependency_resource_acquire" | "dependency_resource_call" | "dependency_resource_dispose" | "cleanup_resource_call" | "context_metadata" | "cleanup_metadata" | "dependency_call" | "effect_register" | "effect_dispose" | "sequence_register" | "sequence_ready" | "sequence_dispose" | "sequence_status" | "scope_register" | "scope_ready" | "scope_run" | "scope_dispose" | "scope_status" | "task_register" | "task_join" | "task_status" | "cleanup_call" | "event_register" | "event_emit" | "event_join" | "event_status" | "event_dispose" | "middleware_register" | "middleware_run" | "middleware_next" | "middleware_dispose" | "child_install" | "child_ready" | "child_settled" | "child_status" | "child_dispose" | "value_provide")
                                    || request.operation.parse::<u64>().ok().is_none_or(|n| n.to_string() != request.operation)
                                    || callback_history.len() >= 65536 || !callback_history.insert(request.operation.clone())
                                { break Err(Error::Protocol("invalid or duplicate reverse request".into())); }
                                let router = callback_router.clone();
                                let workers = match request.command.as_str() { "child_install" | "value_provide" => &mut composition_calls, "child_ready" | "child_settled" => &mut child_waits, "child_dispose" => &mut child_disposals, "effect_dispose" | "sequence_dispose" | "scope_dispose" | "event_dispose" | "middleware_dispose" | "dependency_resource_dispose" => &mut disposals, "sequence_ready" | "scope_ready" => &mut sequence_waits, "scope_run" => &mut scope_runs, "task_join" => &mut task_joins, "cleanup_call" | "cleanup_metadata" | "cleanup_resource_call" => &mut cleanup_calls, "dependency_resource_call" => &mut resource_calls, "event_emit" => &mut event_emits, "event_join" => &mut event_joins, "middleware_run" => &mut middleware_runs, "middleware_next" => &mut middleware_nexts, _ => &mut callbacks };
                                if workers.len() >= CAPACITY {
                                    let response = match reverse_response(&request, Err(Error::Overloaded)) { Ok(bytes) => bytes, Err(error) => break Err(error) };
                                    if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                                } else {
                                    workers.spawn(async move {
                                        let result = router.invoke_payload(&request.command, request.payload.clone()).await;
                                        // A checked dependency result violated its declaration
                                        // after business execution. Do not encode it as rejection.
                                        if let Err(error @ Error::Protocol(_)) = result { return Err(error); }
                                        reverse_response(&request, result)
                                    });
                                }
                                continue;
                            }
                            let Incoming::Response(response) = incoming else { unreachable!() };
                            let Some(entry) = pending.remove(&response.operation) else { break Err(Error::Protocol("unsolicited or duplicate response".into())); };
                            // The operation key was matched by removal above.
                            if entry.session != response.session || entry.activation != response.activation {
                                let error = Error::Protocol("response session/activation mismatch".into()); let _ = entry.reply.send(Err(error.clone())); break Err(error);
                            }
                            let result = outcome(response.outcome);
                            if entry.command == "deactivate" && result.is_ok() { deactivated = true; }
                            let _ = entry.reply.send(result);
                        }
                        Some(Err(error)) => {
                            if deactivated && pending.is_empty() && error == Error::Closed {
                                match timeout(deadline, child.wait()).await {
                                    Ok(Ok(status)) if status.success() => break Ok(()),
                                    other => break Err(Error::HostLost(format!("exit after deactivation: {other:?}"))),
                                }
                            }
                            break Err(Error::HostLost(format!("session disconnected: {error}")));
                        }
                        None => break Err(Error::HostLost("session reader stopped".into())),
                    },
                    result = callbacks.join_next(), if !callbacks.is_empty() => {
                        let response = match result {
                            Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error),
                            _ => break Err(Error::Protocol("reverse dispatcher failed".into())),
                        };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = disposals.join_next(), if !disposals.is_empty() => {
                        let response = match result {
                            Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error),
                            _ => break Err(Error::Protocol("effect dispatcher failed".into())),
                        };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = sequence_waits.join_next(), if !sequence_waits.is_empty() => {
                        let response = match result {
                            Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error),
                            _ => break Err(Error::Protocol("sequence waiter failed".into())),
                        };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = scope_runs.join_next(), if !scope_runs.is_empty() => {
                        let response = match result {
                            Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error),
                            _ => break Err(Error::Protocol("scope runner failed".into())),
                        };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = task_joins.join_next(), if !task_joins.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("task waiter failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = cleanup_calls.join_next(), if !cleanup_calls.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("cleanup dependency dispatcher failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = resource_calls.join_next(), if !resource_calls.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("resource dispatcher failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = event_emits.join_next(), if !event_emits.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("event admission failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = event_joins.join_next(), if !event_joins.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("event join failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = middleware_runs.join_next(), if !middleware_runs.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("middleware run failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = middleware_nexts.join_next(), if !middleware_nexts.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("middleware next failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = composition_calls.join_next(), if !composition_calls.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("composition dispatcher failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = child_waits.join_next(), if !child_waits.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("composition dispatcher failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    result = child_disposals.join_next(), if !child_disposals.is_empty() => {
                        let response = match result { Some(Ok(Ok(response))) => response,
                            Some(Ok(Err(error))) => break Err(error), _ => break Err(Error::Protocol("composition dispatcher failed".into())) };
                        if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                    },
                    _ = tick.tick() => {
                        let now = Instant::now();
                        if pending.values().any(|p| p.deadline.is_some_and(|end| end <= now)) || stopping.is_some_and(|end| end <= now) {
                            break Err(Error::OutcomeUnknown("session deadline exceeded; process terminated without claiming cleanup".into()));
                        }
                    }
                }
            };
            // Resolve failed calls only after the owned process has stopped executing.
            let _ = child.start_kill();
            let reaped = child.wait().await;
            reader.abort();
            writer.abort();
            let _ = reader.await;
            let _ = writer.await;
            let termination = termination.and_then(|()| reaped.map(|_| ()).map_err(Error::from));
            // Even a successful deactivation can leave unsolicited child waits.
            // They must stop before joining reverse workers; native ownership
            // and admitted install/provide operations still drain normally.
            peer_stopped_tx.send_replace(true);
            commands.close();
            // Withdraw a lost host promptly. Pending calls still wait for native
            // work below, whose definition/use-site leases prevent early recovery.
            if termination.is_err() {
                closed_tx.send_replace(Some(termination.clone()));
            }
            // A failed response enqueue loses an already-admitted operation's
            // acknowledgement. Fatal queue exhaustion is HostLost above, never
            // the Overloaded refusal reserved for requests not yet submitted.
            let failure = termination.clone().err().unwrap_or(Error::Closed);
            // Sequence waiters/disposers await step or inverse requests on this
            // session. Wake those before joining callbacks; native continuations
            // still retain their own scope/definition leases.
            let releases: Vec<_> = pending
                .iter()
                .filter(|(_, p)| lane(&p.command) != 0)
                .map(|(id, _)| id.clone())
                .collect();
            for id in releases {
                let _ = pending
                    .remove(&id)
                    .unwrap()
                    .reply
                    .send(Err(failure.clone()));
            }
            while let Some(command) = commands.recv().await {
                if let CommandMessage::Request(_, _, reply) = command {
                    let _ = reply.send(Err(failure.clone()));
                }
            }
            while callbacks.join_next().await.is_some() {}
            while disposals.join_next().await.is_some() {}
            while sequence_waits.join_next().await.is_some() {}
            while scope_runs.join_next().await.is_some() {}
            while task_joins.join_next().await.is_some() {}
            while cleanup_calls.join_next().await.is_some() {}
            while event_emits.join_next().await.is_some() {}
            while event_joins.join_next().await.is_some() {}
            while middleware_runs.join_next().await.is_some() {}
            while middleware_nexts.join_next().await.is_some() {}
            while composition_calls.join_next().await.is_some() {}
            while child_waits.join_next().await.is_some() {}
            while child_disposals.join_next().await.is_some() {}
            while resource_calls.join_next().await.is_some() {}
            for (_, entry) in pending {
                let _ = entry.reply.send(Err(failure.clone()));
            }
            closed_tx.send_replace(Some(termination));
        });
        let session = Self(Arc::new(Client {
            _code: code,
            send,
            closed,
            peer_stopped,
            session,
            activation,
            next: AtomicU64::new(1),
            activated: AtomicBool::new(false),
            timeout: deadline,
            task_stop_timeout,
            router: router.clone(),
        }));
        router.attach(WeakSession(Arc::downgrade(&session.0)));
        Ok(session)
    }

    pub fn envelope(&self, command: &str, payload: impl Into<Payload>) -> Request {
        Request {
            session: self.0.session.clone(),
            activation: self.0.activation.clone(),
            operation: self.0.next.fetch_add(1, Ordering::Relaxed).to_string(),
            command: command.into(),
            payload: payload.into(),
        }
    }
    pub fn scoped_envelope(
        &self,
        context: &Context,
        command: &str,
        payload: impl Into<Payload>,
    ) -> Result<(Request, Option<crate::dependency::Scope>)> {
        let mut payload = payload.into();
        let scope = self.0.router.scope(context, &mut payload.control)?;
        Ok((self.envelope(command, payload), scope))
    }
    pub async fn scoped_payload(
        &self,
        context: &Context,
        command: &str,
        payload: Payload,
    ) -> Result<Payload> {
        let (request, _scope) = self.scoped_envelope(context, command, payload)?;
        self.send(request).await
    }
    pub async fn scoped_request(
        &self,
        context: &Context,
        command: &str,
        payload: Value,
    ) -> Result<Value> {
        let (request, _scope) = self.scoped_envelope(context, command, payload)?;
        self.send(request).await?.control_only().map_err(Into::into)
    }
    pub async fn request(&self, command: &str, payload: Value) -> Result<Value> {
        self.send(self.envelope(command, payload))
            .await?
            .control_only()
            .map_err(Into::into)
    }
    pub async fn send(&self, request: Request) -> Result<Payload> {
        if let Some(result) = self.0.closed.borrow().clone() {
            return Err(result.err().unwrap_or(Error::Closed));
        }
        let permit = self.0.send.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Overloaded,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        let bytes = request
            .encode()
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let (tx, rx) = oneshot::channel();
        permit.send(CommandMessage::Request(request, bytes, tx));
        rx.await.map_err(|_| Error::Closed)?
    }
    pub fn mark_activation_attempted(&self) {
        self.0.activated.store(true, Ordering::Release);
    }
    pub fn enable_composition(&self, enabled: bool) {
        self.0.router.enable_composition(enabled);
    }
    pub fn enable_metadata(&self, enabled: bool) {
        self.0.router.enable_metadata(enabled);
    }
    pub fn enable_dependency_resources(&self, enabled: bool) {
        self.0.router.enable_dependency_resources(enabled);
    }
    pub fn enable_sequences(&self, enabled: bool) {
        self.0.router.enable_sequences(enabled);
    }
    pub fn task_stop_timeout(&self) -> Duration {
        self.0.task_stop_timeout
    }
    pub fn enable_middleware(&self, enabled: bool) {
        self.0.router.enable_middleware(enabled);
    }
    pub fn enable_events(&self, enabled: bool) {
        self.0.router.enable_events(enabled);
    }
    pub fn enable_tasks(&self, enabled: bool) {
        self.0.router.enable_tasks(enabled);
    }
    pub fn enable_scopes(&self, enabled: bool) {
        self.0.router.enable_scopes(enabled);
    }
    pub fn enable_effects(&self, enabled: bool) {
        self.0.router.enable_effects(enabled);
    }
    pub async fn terminated(&self) -> Result<()> {
        let mut rx = self.0.closed.clone();
        loop {
            if let Some(result) = rx.borrow().clone() {
                return result;
            }
            rx.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    pub async fn peer_stopped(&self) {
        let mut rx = self.0.peer_stopped.clone();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
    pub async fn abort(&self, error: Error) -> Result<()> {
        let _ = self.0.send.send(CommandMessage::Abort(error)).await;
        self.terminated().await
    }
    pub async fn close(&self, context: &Context) -> Result<()> {
        if !self.0.activated.load(Ordering::Acquire) {
            let _ = self.0.send.send(CommandMessage::StopUnactivated).await;
            let _ = self.terminated().await;
            // No Activate was sent: only the owned process/transport existed.
            return Ok(());
        }
        let result = self
            .scoped_request(context, "deactivate", Value::Null)
            .await;
        if let Err(error) = result {
            return self.abort(error.clone()).await.and(Err(error));
        }
        self.terminated().await
    }
    pub fn timeout(&self) -> Duration {
        self.0.timeout
    }
}

fn reverse_response(request: &Request, result: Result<Payload>) -> Result<Bytes> {
    request
        .response(crate::dependency::payload_outcome(result))
        .encode()
        .map_err(Into::into)
}
