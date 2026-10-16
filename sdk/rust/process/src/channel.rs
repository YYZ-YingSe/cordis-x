use super::*;
use cordis_wire::{Bytes, Identity, Outcome, Payload, Request, Response};
use tokio::sync::{mpsc, oneshot};
struct Pending {
    request: Request,
    reply: oneshot::Sender<Result<Payload>>,
    lane: usize,
}
pub struct Channel {
    pub validation: Cell<cordis_sdk_core::Validation>,
    pub identity: RefCell<Option<(String, Identity)>>,
    pub dependency_resources: Cell<bool>,
    output: mpsc::Sender<Bytes>,
    pending: RefCell<BTreeMap<String, Pending>>,
    next: Cell<u64>,
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
    pub fn new(output: mpsc::Sender<Bytes>) -> Rc<Self> {
        Rc::new(Self {
            validation: Cell::new(cordis_sdk_core::Validation::Checked),
            identity: RefCell::default(),
            dependency_resources: Cell::new(false),
            output,
            pending: RefCell::default(),
            next: Cell::new(0),
            closed: Cell::new(false),
        })
    }
    pub async fn write(&self, response: Response) -> Result<()> {
        let permit = self.output.reserve().await.map_err(unknown)?;
        permit.send(response.encode().map_err(unknown)?);
        Ok(())
    }
    pub async fn request(&self, command: &str, payload: Value) -> Result<Value> {
        self.request_payload(command, payload.into())
            .await?
            .control_only()
            .map_err(unknown)
    }
    pub async fn request_payload(&self, command: &str, payload: Payload) -> Result<Payload> {
        if self.closed.get() {
            return Err(refused("closed", "Session closed"));
        }
        let lane = match command {
            "effect_dispose"
            | "sequence_dispose"
            | "scope_dispose"
            | "event_dispose"
            | "middleware_dispose"
            | "dependency_resource_dispose" => 1,
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
            "dependency_resource_call" => 13,
            _ => 0,
        };
        if self
            .pending
            .borrow()
            .values()
            .filter(|p| p.lane == lane)
            .count()
            >= 16
        {
            return Err(refused("overloaded", "reverse call capacity reached"));
        }
        if self.next.get() >= 65536 {
            return Err(refused("exhausted", "Session operation history full"));
        }
        let (session, activation) = self
            .identity
            .borrow()
            .clone()
            .ok_or_else(|| refused("closed", "Session not configured"))?;
        self.next.set(self.next.get() + 1);
        let operation = self.next.get().to_string();
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
        let encoded = request.encode().map_err(|e| refused("invalid_value", e))?;
        let (reply, receive) = oneshot::channel();
        self.pending.borrow_mut().insert(
            operation.clone(),
            Pending {
                request,
                reply,
                lane,
            },
        );
        permit.send(encoded);
        receive.await.map_err(unknown)?
    }
    pub fn receive(&self, response: Response) -> Result<()> {
        let pending = self
            .pending
            .borrow_mut()
            .remove(&response.operation)
            .ok_or_else(|| unknown("unsolicited response"))?;
        pending.request.check(&response).map_err(unknown)?;
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
