use crate::{Error, Result, error::outcome, frame};
use cordis_runtime::Context;
use cordis_wire::{Bytes, Identity, Incoming, Payload, Request, Value};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, timeout},
};

use cordis_wire::operations::{Layout, RESERVED_CAPACITY as CAPACITY, REVERSE_LANES};
fn lane(command: &str) -> u8 {
    match command {
        "effect_release" | "sequence_undo" | "sequence_release" | "event_release"
        | "middleware_release" | "resource_release" | "reference_close" => 1,
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
// Requests are the common case in this bounded queue. Keep their admission
// lease inline rather than allocating once more for every ordinary call.
#[allow(clippy::large_enum_variant)]
enum CommandMessage {
    Request(Operation, Bytes, Reply),
    Abort(Error),
    StopUnactivated,
}
pub(crate) struct Operation {
    pub request: Request,
    _lease: cordis_wire::operations::Lease,
    _cleanup_permit: Option<OwnedSemaphorePermit>,
}
struct Pending {
    session: String,
    activation: Identity,
    command: String,
    lease: cordis_wire::operations::Lease,
    cleanup_permit: Option<OwnedSemaphorePermit>,
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
    frame_capacity: usize,
    _code: crate::plugin::CodeLease,
    send: mpsc::Sender<CommandMessage>,
    cleanup_capacity: Arc<Semaphore>,
    closed: watch::Receiver<Option<Result<()>>>,
    peer_stopped: watch::Receiver<bool>,
    session: String,
    activation: Identity,
    operations: Arc<cordis_wire::operations::Outgoing>,
    activated: AtomicBool,
    stop_timeout: Duration,
    router: Arc<crate::dependency::Router>,
}

impl Session {
    pub async fn connect(
        spec: &crate::SessionSpec,
        activation: Identity,
        router: Arc<crate::dependency::Router>,
        connector: Arc<dyn crate::Connector>,
        code: crate::plugin::CodeLease,
    ) -> Result<Self> {
        let layout = Layout::new(spec.max_inflight)
            .filter(|layout| {
                layout.output <= Semaphore::MAX_PERMITS
                    && layout.connections <= Semaphore::MAX_PERMITS
            })
            .ok_or_else(|| {
                Error::InvalidInput("max_inflight must be positive and representable".into())
            })?;
        if spec.io_timeout.as_millis() == 0
            || spec.io_timeout.as_millis() > i32::MAX as u128
            || !spec.io_timeout.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::InvalidInput(
                "I/O timeout must be positive and representable in milliseconds".into(),
            ));
        }
        let ordinary_capacity = spec.max_inflight;
        let frame_capacity = spec.frame_capacity;
        if frame_capacity == 0 || frame_capacity > i32::MAX as usize {
            return Err(Error::InvalidInput(
                "frame_capacity must be positive and fit protobuf length representation".into(),
            ));
        }
        let initialization_timeout = spec.initialization_timeout;
        let execution_timeout = spec.execution_timeout;
        let stop_timeout = spec.stop_timeout;
        if initialization_timeout.is_zero()
            || Instant::now().checked_add(initialization_timeout).is_none()
            || execution_timeout.is_some_and(|timeout| {
                timeout.is_zero() || Instant::now().checked_add(timeout).is_none()
            })
        {
            return Err(Error::Protocol(
                "configured timeouts must be positive and representable".into(),
            ));
        }
        if stop_timeout.is_zero() || Instant::now().checked_add(stop_timeout).is_none() {
            return Err(Error::InvalidInput(
                "stop timeout must be positive and representable".into(),
            ));
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| Error::Io(e.to_string()))?;
        let session: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let connected = timeout(initialization_timeout, connector.connect())
            .await
            .map_err(|_| Error::HostLost("remote connection timed out".into()))??;
        let mut peer = connected.peer;
        let mut stdin =
            tokio::io::BufWriter::with_capacity(frame::BUFFER_CAPACITY, connected.output);
        let mut stdout =
            tokio::io::BufReader::with_capacity(frame::BUFFER_CAPACITY, connected.input);
        let (send, mut commands) = mpsc::channel(ordinary_capacity + CAPACITY);
        let cleanup_capacity = Arc::new(Semaphore::new(CAPACITY));
        let worker_cleanup_capacity = cleanup_capacity.clone();
        let (closed_tx, closed) = watch::channel(None);
        let (peer_stopped_tx, peer_stopped) = watch::channel(false);
        let (writes, mut outgoing) = mpsc::channel::<Bytes>(layout.output);
        let (reads_tx, mut reads) = mpsc::channel(CAPACITY);
        let read_tx = reads_tx.clone();
        let validation = spec.bindings.validation;
        let reader = tokio::spawn(async move {
            loop {
                let result = frame::read(&mut stdout, validation, frame_capacity).await;
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
        let operations = Arc::new(cordis_wire::operations::Outgoing::with_lanes(
            &layout.forward,
        ));
        tokio::spawn(async move {
            let _code = worker_code;
            let mut pending: BTreeMap<String, Pending> = BTreeMap::new();
            let observations = Arc::new(std::sync::Mutex::new(BTreeMap::<
                String,
                watch::Sender<bool>,
            >::new()));
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
            let callback_history =
                cordis_wire::operations::Incoming::new(REVERSE_LANES.iter().sum());
            let mut deactivated = false;
            let mut stopping = None;
            let mut tick = tokio::time::interval(Duration::from_millis(25));
            let termination = loop {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some(CommandMessage::Request(operation, bytes, reply)) => {
                            let request = &operation.request;
                            if stopping.is_some() { drop(operation); let _ = reply.send(Err(Error::Closed)); continue; }
                            let request_timeout = match request.command.as_str() {
                                "hello" | "activate" => Some(initialization_timeout),
                                "deactivate" | "task_cancel" => Some(stop_timeout),
                                "task_call" => None,
                                command if lane(command) == 1 => Some(stop_timeout),
                                _ => execution_timeout,
                            };
                            if request.command == "deactivate" { stopping = Some(Instant::now() + stop_timeout); }
                            if writes.try_send(bytes).is_err() { drop(operation); let _ = reply.send(Err(Error::Overloaded)); continue; }
                            let expires = request_timeout.map(|duration| Instant::now() + duration);
                            let Operation { request, _lease, _cleanup_permit } = operation;
                            pending.insert(request.operation, Pending { session: request.session, activation: request.activation, command: request.command, lease: _lease, cleanup_permit: _cleanup_permit, reply, deadline: expires });
                        }
                        Some(CommandMessage::Abort(error)) => break Err(error),
                        Some(CommandMessage::StopUnactivated) => break Ok(()),
                        None => break Err(Error::Closed),
                    },
                    received = reads.recv() => match received {
                        Some(Ok(incoming)) => {
                            if let Incoming::CancelObservation { session, activation, operation } = &incoming {
                                if session != &callback_session || activation != &callback_activation
                                    || cordis_wire::operations::Id::parse(operation).is_none() {
                                    break Err(Error::Protocol("invalid observation withdrawal identity".into()));
                                }
                                // Registration occurs in this reader before it accepts
                                // another frame, so withdrawal cannot overtake admission.
                                // A business request ID is never in this registry.
                                if let Some(signal) = observations.lock().unwrap().get(operation) { signal.send_replace(true); }
                                continue;
                            }
                            if let Incoming::Request(request) = incoming {
                                if request.session != callback_session || request.activation != callback_activation
                                    || !matches!(request.command.as_str(), "reference_export" | "reference_invoke" | "reference_dispose" | "dependency_resource_acquire" | "dependency_resource_call" | "dependency_resource_dispose" | "cleanup_resource_call" | "context_metadata" | "cleanup_metadata" | "dependency_call" | "effect_register" | "effect_dispose" | "sequence_register" | "sequence_ready" | "sequence_dispose" | "sequence_status" | "scope_register" | "scope_ready" | "scope_run" | "scope_dispose" | "scope_status" | "task_register" | "task_join" | "task_status" | "cleanup_call" | "event_register" | "event_emit" | "event_join" | "event_status" | "event_dispose" | "middleware_register" | "middleware_run" | "middleware_next" | "middleware_dispose" | "child_install" | "child_ready" | "child_settled" | "child_status" | "child_dispose" | "value_provide")

                                { break Err(Error::Protocol("invalid or duplicate reverse request".into())); }
                                let admission = cordis_wire::operations::Id::parse(&request.operation).and_then(|id| callback_history.admit(id));
                                let Some(admission) = admission else { break Err(Error::Protocol("invalid or duplicate reverse request".into())); };
                                let router = callback_router.clone();
                                let workers = match request.command.as_str() { "child_install" | "value_provide" => &mut composition_calls, "child_ready" | "child_settled" => &mut child_waits, "child_dispose" => &mut child_disposals, "effect_dispose" | "sequence_dispose" | "scope_dispose" | "event_dispose" | "middleware_dispose" | "dependency_resource_dispose" | "reference_dispose" => &mut disposals, "sequence_ready" | "scope_ready" => &mut sequence_waits, "scope_run" => &mut scope_runs, "task_join" => &mut task_joins, "cleanup_call" | "cleanup_metadata" | "cleanup_resource_call" => &mut cleanup_calls, "dependency_resource_call" | "reference_invoke" => &mut resource_calls, "event_emit" => &mut event_emits, "event_join" => &mut event_joins, "middleware_run" => &mut middleware_runs, "middleware_next" => &mut middleware_nexts, _ => &mut callbacks };
                                if workers.len() >= CAPACITY {
                                    let response = match reverse_response(&request, Err(Error::Overloaded), frame_capacity) { Ok(bytes) => bytes, Err(error) => break Err(error) };
                                    if writes.try_send(response).is_err() { break Err(Error::HostLost("session response queue exhausted".into())); }
                                } else {
                                    let withdrawal = if cordis_wire::operations::is_observation(&request.command) {
                                        let (send, receive) = watch::channel(false);
                                        observations.lock().unwrap().insert(request.operation.clone(), send);
                                        Some(receive)
                                    } else { None };
                                    let observations = observations.clone();
                                    workers.spawn(async move {
                                        let _admission = admission;
                                        let result = if let Some(mut signal) = withdrawal {
                                            let stop = async { while !*signal.borrow_and_update() {
                                                if signal.changed().await.is_err() { break; }
                                            } };
                                            let result = tokio::select! {
                                                biased;
                                                _ = stop => Err(Error::Refused { code: "observation_cancelled".into(), message: "observation was withdrawn".into() }),
                                                result = router.invoke_payload(&request.command, request.payload.clone()) => result,
                                            };
                                            observations.lock().unwrap().remove(&request.operation);
                                            result
                                        } else { router.invoke_payload(&request.command, request.payload.clone()).await };
                                        // A checked dependency result violated its declaration
                                        // after business execution. Do not encode it as rejection.
                                        if let Err(error @ Error::Protocol(_)) = result { return Err(error); }
                                        reverse_response(&request, result, frame_capacity)
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
                            drop(entry.lease);
                            drop(entry.cleanup_permit);
                            let _ = entry.reply.send(result);
                        }
                        Some(Err(error)) => {
                            if deactivated && pending.is_empty() && error == Error::Closed {
                                match timeout(stop_timeout, peer.wait()).await {
                                    Ok(Ok(())) => break Ok(()),
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
                            break Err(Error::OutcomeUnknown("session deadline exceeded; carrier stopped without claiming cleanup".into()));
                        }
                    }
                }
            };
            // Stop the owned carrier before settling lost results. The peer owner
            // reports what it can actually stop; disconnect never proves rollback.
            let reaped = peer.stop().await;
            reader.abort();
            writer.abort();
            let _ = reader.await;
            let _ = writer.await;
            let termination = termination.and_then(|()| reaped);
            // Even a successful deactivation can leave unsolicited child waits.
            // They must stop before joining reverse workers; native ownership
            // and admitted install/provide operations still drain normally.
            peer_stopped_tx.send_replace(true);
            // Peer loss withdraws observations, not admitted business work.
            for signal in observations.lock().unwrap().values() {
                signal.send_replace(true);
            }
            commands.close();
            worker_cleanup_capacity.close();
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
            frame_capacity,
            _code: code,
            send,
            cleanup_capacity,
            closed,
            peer_stopped,
            session,
            activation,
            operations,
            activated: AtomicBool::new(false),
            stop_timeout,
            router: router.clone(),
        }));
        router.attach(WeakSession(Arc::downgrade(&session.0)));
        Ok(session)
    }

    pub async fn envelope(&self, command: &str, payload: impl Into<Payload>) -> Result<Operation> {
        let cleanup = if lane(command) == 1 {
            Some(
                self.0
                    .cleanup_capacity
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Closed)?,
            )
        } else {
            None
        };
        let lease = self
            .0
            .operations
            .acquire_lane(lane(command).into())
            .ok_or(Error::Overloaded)?;
        Ok(Operation {
            request: Request {
                session: self.0.session.clone(),
                activation: self.0.activation.clone(),
                operation: lease.id.to_string(),
                command: command.into(),
                payload: payload.into(),
            },
            _lease: lease,
            _cleanup_permit: cleanup,
        })
    }
    pub async fn scoped_envelope(
        &self,
        context: &Context,
        command: &str,
        payload: impl Into<Payload>,
    ) -> Result<(Operation, Option<crate::dependency::Scope>)> {
        let mut payload = payload.into();
        let scope = self.0.router.scope(context, &mut payload.control)?;
        Ok((self.envelope(command, payload).await?, scope))
    }
    pub async fn scoped_payload(
        &self,
        context: &Context,
        command: &str,
        payload: Payload,
    ) -> Result<Payload> {
        self.supervise(context, async {
            let (request, _scope) = self.scoped_envelope(context, command, payload).await?;
            self.send(request).await
        })
        .await
    }
    pub async fn scoped_request(
        &self,
        context: &Context,
        command: &str,
        payload: Value,
    ) -> Result<Value> {
        let request = async {
            let (request, _scope) = self.scoped_envelope(context, command, payload).await?;
            self.send(request).await
        };
        let result = if command == "task_call" {
            request.await
        } else {
            self.supervise(context, request).await
        };
        result?.control_only().map_err(Into::into)
    }
    pub async fn supervise<T>(
        &self,
        context: &Context,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let stopping = async {
            if !context.is_cleanup() {
                context.cancelled().await;
            }
            tokio::time::sleep(self.0.stop_timeout).await;
        };
        tokio::pin!(operation);
        tokio::select! {
            biased;
            result = &mut operation => result,
            _ = stopping => {
                let failure = Error::OutcomeUnknown("operation did not drain before the stop deadline".into());
                let _ = self.abort(failure.clone()).await;
                let _ = operation.await;
                Err(failure)
            },
        }
    }
    pub async fn request(&self, command: &str, payload: Value) -> Result<Value> {
        self.send(self.envelope(command, payload).await?)
            .await?
            .control_only()
            .map_err(Into::into)
    }
    pub async fn send(&self, operation: Operation) -> Result<Payload> {
        let request = &operation.request;
        if let Some(result) = self.0.closed.borrow().clone() {
            return Err(result.err().unwrap_or(Error::Closed));
        }
        let cleanup = lane(&request.command) == 1;
        let permit = if cleanup || matches!(request.command.as_str(), "task_cancel" | "deactivate")
        {
            self.0.send.reserve().await.map_err(|_| Error::Closed)?
        } else {
            self.0.send.try_reserve().map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Overloaded,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?
        };
        let bytes = request
            .encode(self.0.frame_capacity)
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let (tx, rx) = oneshot::channel();
        permit.send(CommandMessage::Request(operation, bytes, tx));
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
    pub fn stop_timeout(&self) -> Duration {
        self.0.stop_timeout
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
}

fn reverse_response(request: &Request, result: Result<Payload>, capacity: usize) -> Result<Bytes> {
    request
        .response(crate::dependency::payload_outcome(result))
        .encode(capacity)
        .map_err(Into::into)
}
