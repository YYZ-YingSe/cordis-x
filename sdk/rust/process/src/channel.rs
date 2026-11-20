use super::*;
use cordis_wire::{Bytes, Identity, Outcome, Payload, Request, Response};
use tokio::sync::{mpsc, oneshot};
struct Pending {
    request: Request,
    reply: oneshot::Sender<Result<Payload>>,
}
pub struct Channel {
    pub frame_capacity: Cell<usize>,
    pub io_timeout: Cell<std::time::Duration>,
    pub validation: Cell<cordis_sdk_core::Validation>,
    pub identity: RefCell<Option<(String, Identity)>>,
    pub dependency_resources: Cell<bool>,
    output: mpsc::Sender<Bytes>,
    pending: RefCell<BTreeMap<String, Pending>>,
    operations: cordis_wire::operations::Outgoing,
    closed: Cell<bool>,
}
impl Channel {
    pub fn carrier_validation(&self) -> cordis_sdk_core::Validation {
        if self.identity.borrow().is_some() {
            self.validation.get()
        } else {
            cordis_sdk_core::Validation::Audit
        }
    }
    pub fn new(output: mpsc::Sender<Bytes>, layout: &cordis_wire::operations::Layout) -> Rc<Self> {
        Rc::new(Self {
            frame_capacity: Cell::new(cordis_wire::DEFAULT_FRAME_CAPACITY),
            io_timeout: Cell::new(std::time::Duration::from_secs(5)),
            validation: Cell::new(cordis_sdk_core::Validation::Checked),
            identity: RefCell::default(),
            dependency_resources: Cell::new(false),
            output,
            pending: RefCell::default(),
            operations: cordis_wire::operations::Outgoing::with_lanes(&layout.reverse),
            closed: Cell::new(false),
        })
    }
    pub async fn write(&self, response: Response) -> Result<()> {
        let permit = self.output.reserve().await.map_err(unknown)?;
        permit.send(
            response
                .encode(self.frame_capacity.get())
                .map_err(unknown)?,
        );
        Ok(())
    }
    pub async fn request(&self, command: &str, payload: Value) -> Result<Value> {
        self.request_payload(command, payload.into())
            .await?
            .control_only()
            .map_err(unknown)
    }
    pub async fn request_payload(&self, command: &str, payload: Payload) -> Result<Payload> {
        if cordis_wire::operations::is_observation(command) {
            return Err(refused(
                "observation_required",
                "query requires observation submission",
            ));
        }
        self.submit(command, payload)?.1.await.map_err(unknown)?
    }
    /// The callback bridge owns acknowledgement; its abandonment signal only
    /// withdraws the server observer. The original response retires the slot.
    pub async fn observe(
        &self,
        command: &str,
        payload: Payload,
        abandoned: impl std::future::Future<Output = ()>,
    ) -> Result<Payload> {
        if !cordis_wire::operations::is_observation(command) {
            return Err(refused(
                "invalid_observation",
                "business operations cannot be withdrawn",
            ));
        }
        let (request, mut receive) = self.submit(command, payload)?;
        let result = tokio::select! {
            result = &mut receive => result,
            _ = abandoned => {
                let bytes = request.cancellation(self.frame_capacity.get()).map_err(unknown)?;
                // This control frame consumes no operation slot. Backpressure
                // waits for the writer instead of losing cancellation at capacity.
                self.output.send(bytes).await.map_err(unknown)?;
                receive.await
            }
        };
        result.map_err(unknown)?
    }
    fn submit(
        &self,
        command: &str,
        payload: Payload,
    ) -> Result<(Request, oneshot::Receiver<Result<Payload>>)> {
        if self.closed.get() {
            return Err(refused("closed", "Session closed"));
        }
        let lane = match command {
            "effect_dispose"
            | "sequence_dispose"
            | "scope_dispose"
            | "event_dispose"
            | "middleware_dispose"
            | "dependency_resource_dispose"
            | "reference_dispose" => 1,
            "sequence_ready" | "scope_ready" => 2,
            "scope_run" => 3,
            "task_join" => 4,
            "cleanup_call" | "cleanup_metadata" | "cleanup_resource_call" => 5,
            "event_emit" => 6,
            "event_join" => 7,
            "middleware_run" => 8,
            "middleware_next" => 9,
            "child_ready" | "child_settled" => 10,
            "child_dispose" => 11,
            "child_install" | "value_provide" => 12,
            "dependency_resource_call" | "reference_invoke" => 13,
            _ => 0,
        };
        let (session, activation) = self
            .identity
            .borrow()
            .clone()
            .ok_or_else(|| refused("closed", "Session not configured"))?;
        let operation = self
            .operations
            .allocate_lane(lane)
            .ok_or_else(|| refused("overloaded", "request slots full"))?;
        struct Reservation<'a>(
            &'a cordis_wire::operations::Outgoing,
            Option<cordis_wire::operations::Id>,
        );
        impl Drop for Reservation<'_> {
            fn drop(&mut self) {
                if let Some(id) = self.1 {
                    self.0.complete(id);
                }
            }
        }
        let mut reservation = Reservation(&self.operations, Some(operation));
        let operation = operation.to_string();
        let request = Request {
            session,
            activation,
            operation: operation.clone(),
            command: command.into(),
            payload,
        };
        let permit = self.output.try_reserve().map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                refused("overloaded", "Session output queue full")
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                unknown("Session output queue closed")
            }
        })?;
        let encoded = request
            .encode(self.frame_capacity.get())
            .map_err(|e| refused("invalid_value", e))?;
        let (reply, receive) = oneshot::channel();
        self.pending.borrow_mut().insert(
            operation.clone(),
            Pending {
                request: request.clone(),
                reply,
            },
        );
        permit.send(encoded);
        reservation.1.take();
        Ok((request, receive))
    }
    pub fn receive(&self, response: Response) -> Result<()> {
        let pending = self
            .pending
            .borrow_mut()
            .remove(&response.operation)
            .ok_or_else(|| unknown("unsolicited response"))?;
        pending.request.check(&response).map_err(unknown)?;
        self.operations
            .complete(cordis_wire::operations::Id::parse(&response.operation).unwrap());
        let result = match response.outcome {
            Outcome::Success { value } => Ok(value),
            Outcome::Unknown { error: e } => Err(Failure::Unknown(ErrorInfo {
                code: e.code,
                message: e.message,
            })),
            Outcome::Refused { error: e } => Err(Failure::Refused(ErrorInfo {
                code: e.code,
                message: e.message,
            })),
            // Registration/acquisition use a command-specific acknowledgement.
            // An outer business failure alone does not confirm whether their
            // ownership transfer committed; retain captures in that case.
            Outcome::Failure { error: e } => {
                let info = ErrorInfo {
                    code: e.code,
                    message: e.message,
                };
                Err(
                    if matches!(
                        pending.request.command.as_str(),
                        "effect_register"
                            | "sequence_register"
                            | "scope_register"
                            | "task_register"
                            | "event_register"
                            | "middleware_register"
                            | "child_install"
                            | "value_provide"
                            | "dependency_resource_acquire"
                    ) {
                        Failure::Unknown(info)
                    } else {
                        Failure::Rejected(info)
                    },
                )
            }
        };
        let _ = pending.reply.send(result);
        Ok(())
    }
    pub fn close(&self) {
        self.closed.set(true);
        let pending = std::mem::take(&mut *self.pending.borrow_mut());
        for (_, entry) in pending {
            let _ = entry.reply.send(Err(unknown("Session lost")));
        }
    }
}
