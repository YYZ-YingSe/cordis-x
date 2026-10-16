//! A bounded file input for Loader. The file is the deployment commit point;
//! builds and package publication happen before it is replaced.
use crate::{
    Commit, Configuration, ConfigurationChanges, ConfigurationTarget, Loader, LoaderSnapshot,
};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    future::pending,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct WatchOptions {
    /// A fixed coalescing window, measured from the first outstanding event.
    /// Further events do not indefinitely postpone the next read.
    pub debounce: Duration,
    pub max_bytes: usize,
}
impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(75),
            max_bytes: cordis_wire::MAX_MESSAGE,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchError<E = crate::Error> {
    Options(String),
    Backend(String),
    Read(String),
    Parse(crate::Error),
    Apply(E),
    Worker(String),
    Stopped,
}
impl<E: std::fmt::Display> std::fmt::Display for WatchError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Options(e) => write!(f, "watch options: {e}"),
            Self::Backend(e) => write!(f, "filesystem watcher: {e}"),
            Self::Read(e) => write!(f, "configuration read: {e}"),
            Self::Parse(e) => write!(f, "configuration parse: {e}"),
            Self::Apply(e) => write!(f, "configuration apply: {e}"),
            Self::Worker(e) => write!(f, "configuration worker: {e}"),
            Self::Stopped => f.write_str("configuration watcher stopped"),
        }
    }
}
impl<E: std::error::Error> std::error::Error for WatchError<E> {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchPhase {
    Idle,
    Reading,
    Applying,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct WatchStatus<S = LoaderSnapshot, E = crate::Error> {
    pub phase: WatchPhase,
    pub reads: u64,
    /// Commands accepted by Loader, including commands whose preflight fails.
    pub submissions: u64,
    /// Coalesced filesystem/refresh signal counter, not a configuration revision.
    pub input_revision: u64,
    pub rescans: u64,
    /// Last read/parse/apply error. Unchanged content retains its parse/apply
    /// result; a successful reread can clear a transient file IO error.
    pub error: Option<WatchError<E>>,
    /// Sticky backend failure; a successful read does not prove watches recovered.
    pub backend_error: Option<String>,
    /// Live desired/actual state, independent of the file input phase.
    pub target: S,
}

#[derive(Clone, Default)]
struct Input {
    revision: u64,
    refresh: u64,
    rescans: u64,
    error: Option<String>,
}

/// Watches the containing directory so atomic replacement keeps working.
/// Only pending input is coalesced; an accepted Loader operation is never cancelled.
/// Stop/drop ends input, not plugin lifetimes. Await stop before clearing Loader.
#[must_use = "dropping the watcher stops configuration input"]
pub struct ConfigWatcher<T: ConfigurationTarget = Loader> {
    path: PathBuf,
    watcher: Option<RecommendedWatcher>,
    input: watch::Sender<Input>,
    status: watch::Receiver<WatchStatus<T::Snapshot, T::Error>>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<WatchStatus<T::Snapshot, T::Error>>>,
}
impl<T: ConfigurationTarget> ConfigWatcher<T> {
    /// The containing directory must exist and remain in place. The file itself
    /// may be missing initially. Relative artifact locations keep Registry semantics.
    pub fn start(
        target: T,
        path: impl AsRef<Path>,
        options: WatchOptions,
    ) -> Result<Self, WatchError<T::Error>> {
        if options.max_bytes == 0 || options.max_bytes > cordis_wire::MAX_MESSAGE {
            return Err(WatchError::Options(format!(
                "max_bytes must be in 1..={}",
                cordis_wire::MAX_MESSAGE
            )));
        }
        if Instant::now().checked_add(options.debounce).is_none() {
            return Err(WatchError::Options("debounce exceeds timer range".into()));
        }
        let path = std::path::absolute(path).map_err(|e| WatchError::Read(e.to_string()))?;
        let parent = path
            .parent()
            .ok_or_else(|| WatchError::Read("missing parent directory".into()))?
            .canonicalize()
            .map_err(|e| WatchError::Read(e.to_string()))?;
        let name = path
            .file_name()
            .ok_or_else(|| WatchError::Read("missing file name".into()))?;
        let path = parent.join(name);
        let (input, receiver) = watch::channel(Input::default());
        let events = input.clone();
        let target_path = path.clone();
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
            record_event(&events, &target_path, event);
        })
        .map_err(|e| WatchError::Backend(e.to_string()))?;
        watcher
            .watch(&parent, RecursiveMode::NonRecursive)
            .map_err(|e| WatchError::Backend(e.to_string()))?;
        let initial = WatchStatus {
            phase: WatchPhase::Idle,
            reads: 0,
            submissions: 0,
            input_revision: 0,
            rescans: 0,
            error: None,
            backend_error: None,
            target: target.snapshot(),
        };
        let (status_tx, status) = watch::channel(initial.clone());
        let (stop, stopped) = oneshot::channel();
        let worker = tokio::spawn(run(
            target,
            path.clone(),
            options,
            receiver,
            stopped,
            status_tx,
            initial,
        ));
        Ok(Self {
            path,
            watcher: Some(watcher),
            input,
            status,
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Latest state, not an event log. Clone values promptly; do not hold borrow
    /// guards over await. Runtime transitions also wake subscribers.
    pub fn subscribe(&self) -> watch::Receiver<WatchStatus<T::Snapshot, T::Error>> {
        self.status.clone()
    }

    /// Explicitly re-verify unchanged input (e.g. after repairing a missing package).
    /// Multiple pending refreshes coalesce. There is no automatic failure retry loop.
    pub fn refresh(&self) -> Result<(), WatchError<T::Error>> {
        if self.input.is_closed() {
            return Err(WatchError::Stopped);
        }
        self.input.send_modify(|input| {
            input.revision = input.revision.wrapping_add(1);
            input.refresh = input.refresh.wrapping_add(1);
        });
        Ok(())
    }

    /// Stops ingress, discards unsubmitted input, and joins an accepted apply.
    /// Does not wait for new business initialization or dispose any plugins.
    pub async fn stop(
        mut self,
    ) -> Result<WatchStatus<T::Snapshot, T::Error>, WatchError<T::Error>> {
        self.request_stop();
        self.worker
            .take()
            .expect("owned worker")
            .await
            .map_err(|e| WatchError::Worker(e.to_string()))
    }

    fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.watcher.take();
    }
}
impl<T: ConfigurationTarget> Drop for ConfigWatcher<T> {
    fn drop(&mut self) {
        self.request_stop();
    }
}

fn record_event(input: &watch::Sender<Input>, target: &Path, event: notify::Result<Event>) {
    let (rescan, error) = match event {
        Ok(event) => {
            let rescan = event.need_rescan();
            let writing = !matches!(event.kind, EventKind::Access(_))
                || matches!(
                    event.kind,
                    EventKind::Access(notify::event::AccessKind::Close(
                        notify::event::AccessMode::Write
                    ))
                );
            let relevant = event.paths.is_empty()
                || event
                    .paths
                    .iter()
                    .any(|p| p == target || Some(p.as_path()) == target.parent());
            if !rescan && !(writing && relevant) {
                return;
            }
            (rescan, None)
        }
        Err(error) => (true, Some(error.to_string())),
    };
    input.send_modify(|input| {
        input.revision = input.revision.wrapping_add(1);
        input.rescans = input.rescans.saturating_add(u64::from(rescan));
        if let Some(error) = error {
            input.error = Some(error);
        }
    });
}

#[derive(Debug)]
enum InputError {
    Read(String),
    Parse(crate::Error),
}
impl<E> From<InputError> for WatchError<E> {
    fn from(error: InputError) -> Self {
        match error {
            InputError::Read(e) => Self::Read(e),
            InputError::Parse(e) => Self::Parse(e),
        }
    }
}
type DigestBytes = [u8; 32];
enum ReadResult {
    Unchanged,
    Parsed(DigestBytes, Result<Configuration, InputError>),
}
fn read(
    path: &Path,
    limit: usize,
    previous: Option<DigestBytes>,
) -> Result<ReadResult, InputError> {
    let io = |e: std::io::Error| InputError::Read(format!("{}: {e}", path.display()));
    // Configuration is a regular commit file, not a pipe, directory or symlink.
    if !path.symlink_metadata().map_err(io)?.file_type().is_file() {
        return Err(InputError::Read(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() > limit {
        return Err(InputError::Read(format!(
            "{} exceeds {limit} bytes",
            path.display()
        )));
    }
    let digest = Sha256::digest(&bytes).into();
    if previous == Some(digest) {
        return Ok(ReadResult::Unchanged);
    }
    let config = String::from_utf8(bytes)
        .map_err(|e| InputError::Read(e.to_string()))
        .and_then(|text| Configuration::from_json(&text).map_err(InputError::Parse));
    Ok(ReadResult::Parsed(digest, config))
}

async fn optional<F: std::future::Future + Unpin>(future: &mut Option<F>) -> F::Output {
    match future {
        Some(future) => future.await,
        None => pending().await,
    }
}

async fn run<T: ConfigurationTarget>(
    target: T,
    path: PathBuf,
    options: WatchOptions,
    mut input: watch::Receiver<Input>,
    mut stop: oneshot::Receiver<()>,
    output: watch::Sender<WatchStatus<T::Snapshot, T::Error>>,
    mut status: WatchStatus<T::Snapshot, T::Error>,
) -> WatchStatus<T::Snapshot, T::Error> {
    let mut changes = target.subscribe();
    let mut dirty = Some(Instant::now()); // Register watch before the first read.
    let mut reading: Option<JoinHandle<Result<ReadResult, InputError>>> = None;
    let mut applying: Option<Commit<T::Error>> = None;
    let mut read_input = Input::default();
    let mut digest = None;
    let mut content_error = None;
    let mut refreshed = 0;
    let mut stopping = false;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop, if !stopping => {
                stopping = true;
                dirty = None;
                // Blocking reads cannot be aborted, but their result will never be submitted.
                status.phase = WatchPhase::Stopping;
            }
            result = optional(&mut applying) => {
                applying = None;
                status.error = result.err().map(WatchError::Apply);
                content_error = status.error.clone();
                status.phase = if stopping { WatchPhase::Stopping } else { WatchPhase::Idle };
            }
            result = optional(&mut reading) => {
                reading = None;
                status.phase = if stopping { WatchPhase::Stopping } else { WatchPhase::Idle };
                // A newer notification during the read supersedes this unsubmitted input.
                if !stopping && input.borrow().revision == read_input.revision {
                    refreshed = read_input.refresh;
                    match result.map_err(|e: tokio::task::JoinError| WatchError::Worker(e.to_string())).and_then(|r| r.map_err(WatchError::from)) {
                        Ok(ReadResult::Unchanged) => status.error = content_error.clone(),
                        Ok(ReadResult::Parsed(new_digest, config)) => {
                            digest = Some(new_digest);
                            status.error = None;
                            match config.map_err(WatchError::from).and_then(|config| target.submit(config).map_err(WatchError::Apply)) {
                                Ok(wait) => {
                                    applying = Some(wait);
                                    status.submissions += 1;
                                    status.phase = WatchPhase::Applying;
                                }
                                Err(error) => status.error = Some(error),
                            }
                            content_error = status.error.clone();
                        }
                        Err(error) => status.error = Some(error),
                    }
                }
            }
            _ = async { match dirty { Some(deadline) => tokio::time::sleep_until(deadline).await, None => pending().await } },
                if !stopping && reading.is_none() && applying.is_none() => {
                dirty = None;
                read_input = input.borrow_and_update().clone();
                let previous = if refreshed == read_input.refresh { digest } else { None };
                let path = path.clone();
                let limit = options.max_bytes;
                reading = Some(tokio::task::spawn_blocking(move || read(&path, limit, previous)));
                status.reads += 1;
                status.phase = WatchPhase::Reading;
            }
            result = input.changed(), if !stopping => {
                if result.is_err() { stopping = true; }
                else {
                    input.borrow_and_update();
                    dirty.get_or_insert_with(|| Instant::now() + options.debounce);
                }
            }
            _ = changes.changed() => {}
        }
        {
            let latest = input.borrow();
            status.input_revision = latest.revision;
            status.backend_error = latest.error.clone();
            status.rescans = latest.rescans;
        }
        status.target = target.snapshot();
        if stopping && applying.is_none() && reading.is_none() {
            status.phase = WatchPhase::Stopped;
            output.send_replace(status.clone());
            return status;
        }
        output.send_replace(status.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, AccessMode, Flag, ModifyKind};

    #[test]
    fn notifications_filter_reads_and_unrelated_paths_but_rescan_and_errors_recheck() {
        let (input, _receiver) = watch::channel(Input::default());
        let target = Path::new("/config/current.json");
        for event in [
            Event::new(EventKind::Access(AccessKind::Read)).add_path(target.into()),
            Event::new(EventKind::Access(AccessKind::Close(AccessMode::Read)))
                .add_path(target.into()),
            Event::new(EventKind::Modify(ModifyKind::Any))
                .add_path("/config/unrelated.json".into()),
        ] {
            record_event(&input, target, Ok(event));
        }
        assert_eq!(input.borrow().revision, 0);
        for event in [
            Event::new(EventKind::Modify(ModifyKind::Any)).add_path(target.into()),
            Event::new(EventKind::Access(AccessKind::Close(AccessMode::Write)))
                .add_path(target.into()),
            Event::new(EventKind::Other).set_flag(Flag::Rescan),
        ] {
            record_event(&input, target, Ok(event));
        }
        record_event(&input, target, Err(notify::Error::generic("events lost")));
        assert_eq!(input.borrow().revision, 4);
        assert_eq!(input.borrow().rescans, 2);
        assert!(
            input
                .borrow()
                .error
                .as_ref()
                .unwrap()
                .contains("events lost")
        );
    }

    #[test]
    fn read_bounds_are_inclusive_and_nonregular_files_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let bytes = br#"{"entries":[]}"#;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            read(&path, bytes.len(), None),
            Ok(ReadResult::Parsed(_, Ok(_)))
        ));
        assert!(matches!(
            read(&path, bytes.len() - 1, None),
            Err(InputError::Read(_))
        ));
        assert!(matches!(
            read(dir.path(), 100, None),
            Err(InputError::Read(_))
        ));
        #[cfg(unix)]
        {
            let link = dir.path().join("link.json");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(matches!(read(&link, 100, None), Err(InputError::Read(_))));
        }
    }
}
