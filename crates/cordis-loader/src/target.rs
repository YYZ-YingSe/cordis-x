//! Configuration input is independent of ordinary reconciliation or rolling deployment.
use crate::{Configuration, Loader, LoaderChanges, LoaderSnapshot, Result};
use cordis_runtime::BoxFuture;
use std::future::Future;

pub type Commit<E> = BoxFuture<'static, std::result::Result<(), E>>;

pub trait ConfigurationChanges: Send {
    fn changed(&mut self) -> impl Future<Output = Result<()>> + Send;
}
impl ConfigurationChanges for LoaderChanges {
    async fn changed(&mut self) -> Result<()> {
        self.changed().await
    }
}

/// Synchronous admission followed by an owned completion wait. Once admitted,
/// dropping the wait must not cancel work. Snapshots describe actual state, and
/// changes are coalesced wake-ups rather than an audit log.
pub trait ConfigurationTarget: Clone + Send + Sync + 'static {
    type Snapshot: Clone + Send + Sync + 'static;
    type Error: std::error::Error + Clone + Send + Sync + 'static;
    type Changes: ConfigurationChanges;
    fn snapshot(&self) -> Self::Snapshot;
    fn subscribe(&self) -> Self::Changes;
    fn submit(
        &self,
        config: Configuration,
    ) -> std::result::Result<Commit<Self::Error>, Self::Error>;
}
impl ConfigurationTarget for Loader {
    type Snapshot = LoaderSnapshot;
    type Error = crate::Error;
    type Changes = LoaderChanges;
    fn snapshot(&self) -> Self::Snapshot {
        self.snapshot()
    }
    fn subscribe(&self) -> Self::Changes {
        self.subscribe()
    }
    fn submit(&self, config: Configuration) -> Result<Commit<Self::Error>> {
        let wait = self.submit(config)?;
        Ok(Box::pin(async move {
            wait.await.map_err(|_| crate::Error::Stopped)??;
            Ok(())
        }))
    }
}
