//! Connected byte carriers for the common remote Session. Carriers own I/O and
//! peer lifetime; protocol dispatch and resource responsibility stay in Session.
use crate::{Error, Result};
use cordis_runtime::BoxFuture;
use std::{path::PathBuf, process::Stdio};
use tokio::io::{AsyncRead, AsyncWrite};

pub type Input = Box<dyn AsyncRead + Unpin + Send>;
pub type Output = Box<dyn AsyncWrite + Unpin + Send>;

/// The embedding owns the peer. Processes must be reaped; an external browser
/// carrier must be closed by its embedding. A successful stop never asserts
/// that remote business effects were rolled back or that an unowned OS process
/// was terminated. Lost responses retain the Session's unknown outcome.
pub trait Peer: Send {
    fn wait(&mut self) -> BoxFuture<'_, Result<()>>;
    fn stop(&mut self) -> BoxFuture<'_, Result<()>>;
}

pub struct Connected {
    pub input: Input,
    pub output: Output,
    pub peer: Box<dyn Peer>,
}

/// Establish one carrier per activation. Cancelling establishment must release
/// partially acquired transport resources. Call and resource operations are
/// admitted only after the common Session handshake completes.
pub trait Connector: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<Connected>>;
}

pub(crate) struct ProcessConnector {
    pub executable: PathBuf,
    pub args: Vec<String>,
}
impl Connector for ProcessConnector {
    fn connect(&self) -> BoxFuture<'_, Result<Connected>> {
        Box::pin(async {
            let mut child = tokio::process::Command::new(&self.executable)
                .args(&self.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?;
            Ok(Connected {
                input: Box::new(child.stdout.take().expect("piped stdout")),
                output: Box::new(child.stdin.take().expect("piped stdin")),
                peer: Box::new(ProcessPeer(child)),
            })
        })
    }
}
struct ProcessPeer(tokio::process::Child);
impl Peer for ProcessPeer {
    fn wait(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let status = self.0.wait().await?;
            if status.success() {
                Ok(())
            } else {
                Err(Error::HostLost(format!("peer exited {status}")))
            }
        })
    }
    fn stop(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            let _ = self.0.start_kill();
            self.0.wait().await?;
            Ok(())
        })
    }
}
