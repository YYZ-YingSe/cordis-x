//! One independently deployable Broker implementation, with explicit business readiness.
//! A private Loader owns generations; the stable Broker and consumers live outside it.
use crate::{
    Commit, Configuration, ConfigurationChanges, ConfigurationTarget, Entry, Loader, LoaderChanges,
    LoaderSnapshot, Registry,
};
use cordis_kernel::{Activation, Lifecycle};
use cordis_runtime::{
    BoxFuture, Broker, BrokerRegistration, Context, Runtime, ServiceHandle, WeakBrokerRegistration,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};

/// Weak, activation-keyed callbacks from existing native/ABI/Process/Wasm factories.
/// One implementation may register one candidate per activation. Separate deployment
/// units use separate tables. The table never keeps retired code alive.
pub struct Candidates<T: ?Sized> {
    entries: Arc<Mutex<BTreeMap<Activation, WeakBrokerRegistration<T>>>>,
}
impl<T: ?Sized> Clone for Candidates<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}
impl<T: ?Sized> Default for Candidates<T> {
    fn default() -> Self {
        Self {
            entries: Arc::default(),
        }
    }
}
impl<T: ?Sized + Send + Sync + 'static> Candidates<T> {
    pub fn register(&self, candidate: BrokerRegistration<T>) -> cordis_runtime::Result<()> {
        let activation = candidate
            .activation()
            .ok_or(cordis_runtime::Error::Closed)?;
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, candidate| candidate.upgrade().is_some());
        if entries.contains_key(&activation) {
            return Err(cordis_runtime::Error::Message(
                "duplicate candidate for activation".into(),
            ));
        }
        // A normal deployment has at most two generations. This also bounds
        // malformed factory callbacks and externally retained registration tokens.
        if entries.len() >= 16 {
            return Err(cordis_runtime::Error::Message(
                "candidate table full".into(),
            ));
        }
        entries.insert(activation, candidate.downgrade());
        Ok(())
    }
    fn get(&self, activation: Activation) -> Option<BrokerRegistration<T>> {
        self.entries
            .lock()
            .unwrap()
            .get(&activation)
            .and_then(WeakBrokerRegistration::upgrade)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    NotReady,
}

#[derive(Clone, Debug)]
pub struct ReadinessPolicy {
    /// One total deadline for activation and all probes, excluding preparation/recovery.
    pub timeout: Duration,
    pub interval: Duration,
    pub consecutive_successes: usize,
}
impl Default for ReadinessPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            interval: Duration::from_millis(100),
            consecutive_successes: 1,
        }
    }
}
impl ReadinessPolicy {
    fn validate(&self) -> Result<(), DeploymentError> {
        if self.timeout.is_zero()
            || self.interval.is_zero()
            || self.consecutive_successes == 0
            || Instant::now().checked_add(self.timeout).is_none()
            || Instant::now().checked_add(self.interval).is_none()
        {
            return Err(DeploymentError::Invalid(
                "readiness requires finite positive timeout, interval and success count".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeploymentError {
    Invalid(String),
    Prepare(crate::Error),
    Candidate(String),
    Probe(cordis_runtime::Error),
    Timeout,
    Promote(cordis_runtime::Error),
    Drain(crate::Error),
    Blocked(crate::Error),
    QueueFull,
    Stopped,
}
impl std::fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(e) => write!(f, "invalid deployment: {e}"),
            Self::Prepare(e) => write!(f, "candidate preparation: {e}"),
            Self::Candidate(e) => write!(f, "candidate activation: {e}"),
            Self::Probe(e) => write!(f, "readiness probe: {e}"),
            Self::Timeout => f.write_str("candidate readiness deadline exceeded"),
            Self::Promote(e) => write!(f, "candidate promotion: {e}"),
            Self::Drain(e) => write!(f, "retired generation recovery: {e}"),
            Self::Blocked(e) => write!(
                f,
                "deployment blocked by retained recovery responsibility: {e}"
            ),
            Self::QueueFull => f.write_str("deployment queue full"),
            Self::Stopped => f.write_str("deployment controller stopped"),
        }
    }
}
impl std::error::Error for DeploymentError {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeploymentPhase {
    #[default]
    Idle,
    Installing,
    WaitingForActive,
    CheckingReadiness,
    Rejecting,
    Draining,
    Failed,
}
#[derive(Clone, Debug)]
pub struct Generation {
    pub id: String,
    pub entry: Entry,
    pub activation: Option<Activation>,
}
#[derive(Clone, Debug, Default)]
pub struct DeploymentState {
    pub revision: u64,
    pub phase: DeploymentPhase,
    pub desired: Option<Entry>,
    /// Most recently promoted generation, not a claim of perpetual health.
    pub current: Option<Generation>,
    pub candidate: Option<Generation>,
    pub error: Option<DeploymentError>,
    /// Separate from the triggering rejection; never hidden by a timeout/probe error.
    pub cleanup_error: Option<crate::Error>,
}
#[derive(Clone, Debug)]
pub struct DeploymentSnapshot {
    pub deployment: DeploymentState,
    pub loader: LoaderSnapshot,
}

pub struct DeploymentChanges {
    deployment: watch::Receiver<()>,
    loader: LoaderChanges,
}
impl ConfigurationChanges for DeploymentChanges {
    async fn changed(&mut self) -> crate::Result<()> {
        tokio::select! {
            result = self.deployment.changed() => result.map_err(|_| crate::Error::Stopped),
            result = self.loader.changed() => result,
        }
    }
}

type Probe<T> = dyn for<'a> Fn(&'a T, Context) -> BoxFuture<'a, cordis_runtime::Result<Readiness>>
    + Send
    + Sync;
type Reply = oneshot::Sender<Result<DeploymentSnapshot, DeploymentError>>;

/// A bounded FIFO deployment controller for a single enabled leaf Entry (or an
/// empty/disabled desired state). It exclusively owns its Loader and promotions.
/// Drop does not dispose plugins; submit an empty configuration before shutdown.
pub struct RollingDeployment<T: ?Sized> {
    commands: mpsc::Sender<(Configuration, Reply)>,
    loader: Loader,
    state: Arc<Mutex<DeploymentState>>,
    changed: watch::Sender<()>,
    marker: std::marker::PhantomData<fn() -> T>,
}
impl<T: ?Sized> Clone for RollingDeployment<T> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            loader: self.loader.clone(),
            state: self.state.clone(),
            changed: self.changed.clone(),
            marker: std::marker::PhantomData,
        }
    }
}
impl<T: ?Sized + Send + Sync + 'static> RollingDeployment<T> {
    pub fn new<F>(
        runtime: Runtime,
        registry: Registry,
        candidates: Candidates<T>,
        client: ServiceHandle<Broker<T>>,
        policy: ReadinessPolicy,
        probe: F,
    ) -> Result<Self, DeploymentError>
    where
        F: for<'a> Fn(&'a T, Context) -> BoxFuture<'a, cordis_runtime::Result<Readiness>>
            + Send
            + Sync
            + 'static,
    {
        policy.validate()?;
        let loader = Loader::new(runtime, registry);
        let state = Arc::new(Mutex::new(DeploymentState::default()));
        let (changed, _) = watch::channel(());
        let (commands, mut requests) = mpsc::channel::<(Configuration, Reply)>(16);
        let mut worker = Worker {
            loader: loader.clone(),
            state: state.clone(),
            changed: changed.clone(),
            candidates,
            client,
            policy,
            probe: Arc::new(probe),
            current: None,
        };
        tokio::spawn(async move {
            while let Some((config, reply)) = requests.recv().await {
                let result = worker.apply(config).await;
                if let Err(error) = &result {
                    worker.update(|state| {
                        state.error = Some(error.clone());
                        state.phase = DeploymentPhase::Failed;
                    });
                }
                let result = result.map(|_| DeploymentSnapshot {
                    deployment: worker.state.lock().unwrap().clone(),
                    loader: worker.loader.snapshot(),
                });
                let _ = reply.send(result);
            }
        });
        Ok(Self {
            commands,
            loader,
            state,
            changed,
            marker: std::marker::PhantomData,
        })
    }
    pub async fn apply(
        &self,
        config: Configuration,
    ) -> Result<DeploymentSnapshot, DeploymentError> {
        self.enqueue(config)?
            .await
            .map_err(|_| DeploymentError::Stopped)?
    }
    fn enqueue(
        &self,
        config: Configuration,
    ) -> Result<oneshot::Receiver<Result<DeploymentSnapshot, DeploymentError>>, DeploymentError>
    {
        let (reply, wait) = oneshot::channel();
        self.commands
            .try_send((config, reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => DeploymentError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => DeploymentError::Stopped,
            })?;
        Ok(wait)
    }
    pub fn snapshot(&self) -> DeploymentSnapshot {
        DeploymentSnapshot {
            deployment: self.state.lock().unwrap().clone(),
            loader: self.loader.snapshot(),
        }
    }
    pub fn subscribe(&self) -> DeploymentChanges {
        DeploymentChanges {
            deployment: self.changed.subscribe(),
            loader: self.loader.subscribe(),
        }
    }
}
impl<T: ?Sized + Send + Sync + 'static> ConfigurationTarget for RollingDeployment<T> {
    type Snapshot = DeploymentSnapshot;
    type Error = DeploymentError;
    type Changes = DeploymentChanges;
    fn snapshot(&self) -> Self::Snapshot {
        self.snapshot()
    }
    fn subscribe(&self) -> Self::Changes {
        self.subscribe()
    }
    fn submit(&self, config: Configuration) -> Result<Commit<Self::Error>, Self::Error> {
        let wait = self.enqueue(config)?;
        Ok(Box::pin(async move {
            wait.await.map_err(|_| DeploymentError::Stopped)??;
            Ok(())
        }))
    }
}

struct Current<T: ?Sized> {
    generation: Generation,
    actual: Entry,
    registration: BrokerRegistration<T>,
}
struct Worker<T: ?Sized> {
    loader: Loader,
    state: Arc<Mutex<DeploymentState>>,
    changed: watch::Sender<()>,
    candidates: Candidates<T>,
    client: ServiceHandle<Broker<T>>,
    policy: ReadinessPolicy,
    probe: Arc<Probe<T>>,
    current: Option<Current<T>>,
}
impl<T: ?Sized + Send + Sync + 'static> Worker<T> {
    fn update(&self, change: impl FnOnce(&mut DeploymentState)) {
        change(&mut self.state.lock().unwrap());
        self.changed.send_replace(());
    }
    fn base(&self) -> Configuration {
        Configuration {
            entries: self.current.iter().map(|c| c.actual.clone()).collect(),
        }
    }
    async fn apply(&mut self, config: Configuration) -> Result<(), DeploymentError> {
        if config.entries.len() > 1 {
            return Err(DeploymentError::Invalid(
                "one deployment owns one leaf entry".into(),
            ));
        }
        let desired = config.entries.into_iter().next();
        if let Some(entry) = &desired
            && (entry.id.trim().is_empty()
                || !entry.children.is_empty()
                || entry.source == crate::config::GROUP)
        {
            return Err(DeploymentError::Invalid(
                "deployment requires a named leaf implementation".into(),
            ));
        }
        let enabled = desired.as_ref().is_some_and(|entry| entry.enabled);
        let blocked = self.state.lock().unwrap().cleanup_error.clone();
        if enabled && let Some(error) = blocked {
            return Err(DeploymentError::Blocked(error));
        }
        self.update(|state| {
            state.revision += 1;
            state.desired = desired.clone();
            state.error = None;
            state.phase = DeploymentPhase::Installing;
        });
        if !enabled {
            if let Some(entry) = desired {
                self.loader
                    .plan(Configuration {
                        entries: vec![entry],
                    })
                    .await
                    .map_err(DeploymentError::Prepare)?;
            }
            self.update(|state| state.phase = DeploymentPhase::Draining);
            if let Err(error) = self.loader.apply(Configuration::default()).await {
                self.update(|state| state.cleanup_error = Some(error.clone()));
                return Err(DeploymentError::Drain(error));
            }
            self.current = None;
            self.update(|state| {
                state.current = None;
                state.candidate = None;
                state.cleanup_error = None;
                state.phase = DeploymentPhase::Idle;
            });
            return Ok(());
        }
        let desired = desired.expect("enabled entry");
        // An explicit new request may repair a lost serving activation. Do not
        // include its failed definition in a staging configuration: ordinary
        // Loader reconciliation would restart it behind the saved route token.
        let current_live = self.current.as_ref().is_none_or(|current| {
            self.loader.snapshot().entries.iter().any(|entry| {
                entry.id == current.actual.id
                    && entry.instance.as_ref().is_some_and(|s| {
                        s.state == Lifecycle::Active
                            && s.activation == current.generation.activation
                    })
            })
        });
        if !current_live {
            self.update(|state| state.phase = DeploymentPhase::Draining);
            if let Err(error) = self.loader.apply(Configuration::default()).await {
                self.update(|state| state.cleanup_error = Some(error.clone()));
                return Err(DeploymentError::Drain(error));
            }
            self.current = None;
            self.update(|state| {
                state.current = None;
                state.phase = DeploymentPhase::Installing;
            });
        }
        if let Some(current) = &self.current
            && current.generation.entry == desired
            && self.loader.snapshot().entries.iter().any(|entry| {
                entry.id == current.actual.id
                    && entry.instance.as_ref().is_some_and(|s| {
                        s.state == Lifecycle::Active
                            && s.activation == current.generation.activation
                    })
            })
        {
            self.loader
                .apply(self.base())
                .await
                .map_err(DeploymentError::Prepare)?;
            current
                .registration
                .promote_if_current(Some(&current.registration))
                .map_err(DeploymentError::Promote)?;
            self.update(|state| state.phase = DeploymentPhase::Idle);
            return Ok(());
        }
        let revision = self.state.lock().unwrap().revision;
        let mut actual = desired.clone();
        actual.id = format!("{}@{revision}", desired.id);
        let generation = Generation {
            id: actual.id.clone(),
            entry: desired,
            activation: None,
        };
        self.update(|state| state.candidate = Some(generation.clone()));
        let mut staged = self.base();
        staged.entries.push(actual.clone());
        if let Err(error) = self.loader.apply(staged).await {
            return self.reject(DeploymentError::Prepare(error)).await;
        }
        let deadline = Instant::now() + self.policy.timeout;
        let candidate = match tokio::time::timeout_at(deadline, self.ready(&actual.id)).await {
            Ok(Ok(candidate)) => candidate,
            Ok(Err(error)) => return self.reject(error).await,
            Err(_) => return self.reject(DeploymentError::Timeout).await,
        };
        if let Err(error) =
            candidate.promote_if_current(self.current.as_ref().map(|c| &c.registration))
        {
            return self.reject(DeploymentError::Promote(error)).await;
        }
        let mut generation = generation;
        generation.activation = candidate.activation();
        self.current = Some(Current {
            generation: generation.clone(),
            actual,
            registration: candidate,
        });
        self.update(|state| {
            state.current = Some(generation);
            state.candidate = None;
            state.phase = DeploymentPhase::Draining;
        });
        // New traffic is already on the candidate. Old responsibility remains in
        // the private Loader until its ordinary retirement and recovery succeed.
        if let Err(error) = self.loader.apply(self.base()).await {
            self.update(|state| state.cleanup_error = Some(error.clone()));
            return Err(DeploymentError::Drain(error));
        }
        self.update(|state| state.phase = DeploymentPhase::Idle);
        Ok(())
    }
    async fn reject(&self, error: DeploymentError) -> Result<(), DeploymentError> {
        self.update(|state| {
            state.error = Some(error.clone());
            state.phase = DeploymentPhase::Rejecting;
        });
        match self.loader.apply(self.base()).await {
            Ok(_) => self.update(|state| state.candidate = None),
            Err(cleanup) => self.update(|state| state.cleanup_error = Some(cleanup)),
        }
        Err(error)
    }
    async fn ready(&self, id: &str) -> Result<BrokerRegistration<T>, DeploymentError> {
        self.update(|state| state.phase = DeploymentPhase::WaitingForActive);
        let mut changes = self.loader.subscribe();
        let candidate = loop {
            let snapshot = self.loader.snapshot();
            let instance = snapshot
                .entries
                .iter()
                .find(|e| e.id == id)
                .and_then(|e| e.instance.as_ref())
                .ok_or_else(|| DeploymentError::Candidate("candidate disappeared".into()))?;
            if let Some(outcome) = &instance.outcome {
                return Err(DeploymentError::Candidate(format!("{outcome:?}")));
            }
            if instance.state == Lifecycle::Active {
                let activation = instance.activation.ok_or_else(|| {
                    DeploymentError::Candidate("active candidate has no activation".into())
                })?;
                let candidate = self.candidates.get(activation).ok_or_else(|| {
                    DeploymentError::Candidate(
                        "active implementation did not register a candidate".into(),
                    )
                })?;
                self.update(|state| {
                    if let Some(candidate) = &mut state.candidate {
                        candidate.activation = Some(activation);
                    }
                });
                break candidate;
            }
            changes.changed().await.map_err(DeploymentError::Prepare)?;
        };
        self.update(|state| state.phase = DeploymentPhase::CheckingReadiness);
        let mut successes = 0;
        loop {
            let probe = self.probe.clone();
            match self
                .client
                .probe(&candidate, move |value, context| probe(value, context))
                .await
                .map_err(DeploymentError::Probe)?
            {
                Readiness::Ready => successes += 1,
                Readiness::NotReady => successes = 0,
            }
            if successes >= self.policy.consecutive_successes {
                return Ok(candidate);
            }
            tokio::time::sleep(self.policy.interval).await;
        }
    }
}
