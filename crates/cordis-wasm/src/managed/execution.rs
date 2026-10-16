//! A Store has one driver; asynchronous exports progress concurrently inside it.
//! Messages admit calls, while native component events drive their continuations.
use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::{mpsc, oneshot};
use wasmtime::AsContextMut;

pub(super) struct Call {
    pub token: String,
    pub cleanup: bool,
    pub command: String,
    pub args: wit::Payload,
    pub context: Context,
    pub background: bool,
    pub reply: oneshot::Sender<Result<Payload>>,
}
pub(super) enum Message {
    Call(Box<Call>),
    Stop(oneshot::Sender<()>),
}
pub(super) fn start(instance: &Arc<Instance>, guest: Guest) {
    let (send, mut receive) = mpsc::unbounded_channel();
    *instance.worker.lock().unwrap() = Some(send);
    let weak = Arc::downgrade(instance);
    let timeout = instance.module.capabilities.call_timeout;
    let fuel = instance.module.limits.fuel_per_call;
    let validation = instance.module.capabilities.validation;
    tokio::spawn(async move {
        let (expired, mut expiration) = mpsc::unbounded_channel::<Error>();
        let Guest {
            mut store,
            bindings,
        } = guest;
        let mut stopping = None;
        let driving = store.run_concurrent(async |accessor| -> Result<()> {
            type Completion<'a> = BoxFuture<'a, (oneshot::Sender<Result<Payload>>, Result<Payload>)>;
            let mut calls: FuturesUnordered<Completion<'_>> = FuturesUnordered::new();
            loop {
                if stopping.is_some() && calls.is_empty() { return Ok(()); }
                tokio::select! {
                    message = receive.recv(), if stopping.is_none() => {
                        match message {
                            Some(Message::Call(call)) => {
                                let call = *call;
                                // The finite Store budget bounds CPU consumption.
                                // Refill only at admission, never in a guest loop.
                                accessor.with(|mut access| access.as_context_mut().set_fuel(fuel)).map_err(unknown)?;
                                let guest = bindings.cordis_plugin_managed_service();
                                let expired = expired.clone();
                                let cancellation = call.context.clone();
                                let watchdog = tokio::spawn(async move {
                                    if call.background { cancellation.cancelled().await; }
                                    tokio::time::sleep(timeout).await;
                                    let _ = expired.send(unknown("guest call deadline exceeded"));
                                });
                                calls.push(Box::pin(async move {
                                    struct Deadline(tokio::task::JoinHandle<()>);
                                    impl Drop for Deadline { fn drop(&mut self) { self.0.abort(); } }
                                    let _deadline = Deadline(watchdog);
                                    let invoke = guest.call_call(accessor, call.token, call.cleanup,
                                                                 call.command, call.args);
                                    let result = invoke.await
                                        .map_err(|error| unknown(format!("{error:#}")))
                                        .and_then(business).and_then(|value| output(value, validation));
                                    (call.reply, result)
                                }));
                            }
                            Some(Message::Stop(reply)) => stopping = Some(reply),
                            None => return Err(unknown("component driver closed before retirement")),
                        }
                    }
                    Some((reply, result)) = calls.next(), if !calls.is_empty() => {
                        let fault = match &result { Err(error @ Error::Unknown(_)) => Some(error.clone()), _ => None };
                        let _ = reply.send(result);
                        if let Some(error) = fault { return Err(error); }
                    }
                }
            }
        });
        // A synchronous WASI import can suspend the whole Store. Its deadline
        // must live outside run_concurrent, so it remains observable even when
        // the Store cannot poll its application future.
        let result = {
            tokio::pin!(driving);
            tokio::select! {
                result = &mut driving => result.map_err(|error| unknown(format!("{error:#}"))).and_then(|result| result),
                Some(error) = expiration.recv() => Err(error),
            }
        };
        if let Some(instance) = weak.upgrade() {
            if let Err(error) = result {
                instance.fault.lock().unwrap().get_or_insert(error);
                // A trapped/timed-out Store may still own guest frames, host
                // futures and uncertain resource acquisitions. Retain it with
                // the failed Runtime contribution; do not claim successful exit.
                *instance.guest.lock().unwrap() = Some(Guest { store, bindings });
            }
            instance.worker.lock().unwrap().take();
        }
        if let Some(reply) = stopping {
            let _ = reply.send(());
        }
    });
}
