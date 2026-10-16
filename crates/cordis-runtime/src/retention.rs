//! Dynamic lifetime edges retain whole implementations, not just Rust objects.
use crate::{Context, Error, Result, gate::Lease, runtime::ActivationData};
use cordis_kernel::Activation;
use std::sync::{Arc, Mutex, Weak};

pub(crate) struct Pin {
    pub provider: Activation,
    pub owner: Activation,
    pub(crate) durable: bool,
    source: Weak<ActivationData>,
    lease: Mutex<Option<Lease>>,
    pub failure: Mutex<Option<Error>>,
}

impl Pin {
    pub fn enter(&self) -> Result<Lease> {
        let lease = self.lease.lock().unwrap();
        if lease.is_none() {
            return Err(Error::Closed);
        }
        self.source
            .upgrade()
            .ok_or(Error::Closed)?
            .gate
            .enter(false, true)
    }

    pub fn live(&self) -> bool {
        self.lease.lock().unwrap().is_some()
    }

    fn new(
        context: &Context,
        source: &Arc<ActivationData>,
        lease: Lease,
        durable: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider: source.activation,
            owner: context.owner(),
            durable,
            source: Arc::downgrade(source),
            lease: Mutex::new(Some(lease)),
            failure: Mutex::new(None),
        })
    }
}

pub(crate) struct Pins(pub Vec<Arc<Pin>>);
impl Drop for Pins {
    fn drop(&mut self) {
        for pin in &self.0 {
            pin.lease.lock().unwrap().take();
        }
    }
}

impl Context {
    pub(crate) fn retain_dynamic(&self, durable: bool) -> Result<(Self, Pins)> {
        self.check_authority()?;
        if self.dynamic.is_empty() {
            return Ok((self.clone(), Pins(Vec::new())));
        }
        let inner = self.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        // Same ordering as lifecycle diagnostics: coordinator, then pin registry.
        let state = inner.state.lock().unwrap();
        let mut registry = inner.retained.lock().unwrap();
        registry.retain(|pin| pin.upgrade().is_some_and(|pin| pin.live()));
        if durable {
            // A lifetime edge must not introduce a recovery cycle. Calls themselves
            // are transient; arbitrary synchronous application wait cycles remain
            // the application's responsibility.
            let reachable = crate::runtime::retention_waits(&state, &registry, self.owner().fiber);
            if self
                .dynamic
                .iter()
                .any(|pin| !pin.durable && reachable.contains(&pin.provider.fiber))
            {
                return Err(Error::WouldDeadlock);
            }
        }
        let mut pins = Pins(Vec::new());
        for previous in &self.dynamic {
            let lease = previous.enter()?;
            let source = previous.source.upgrade().ok_or(Error::Closed)?;
            let pin = Pin::new(self, &source, lease, durable || previous.durable);
            registry.push(Arc::downgrade(&pin));
            pins.0.push(pin);
        }
        let mut context = self.clone();
        context.dynamic = pins.0.clone();
        Ok((context, pins))
    }

    pub(crate) fn retain_provider(
        &mut self,
        source: &Arc<ActivationData>,
        lease: Lease,
        pins: &mut Pins,
    ) -> Result<()> {
        let inner = self.inner.upgrade().ok_or(Error::RuntimeStopped)?;
        let pin = Pin::new(self, source, lease, false);
        let mut registry = inner.retained.lock().unwrap();
        registry.retain(|pin| pin.upgrade().is_some_and(|pin| pin.live()));
        registry.push(Arc::downgrade(&pin));
        self.dynamic.push(pin.clone());
        pins.0.push(pin);
        Ok(())
    }

    pub(crate) fn definition_lease(&self) -> Result<Lease> {
        if let Some(pin) = self
            .dynamic
            .iter()
            .find(|p| p.provider == self.activation())
        {
            pin.enter()
        } else {
            self.definition.gate.enter(false, self.continuing())
        }
    }
}
