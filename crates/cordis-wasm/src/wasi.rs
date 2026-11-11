//! Explicit WASI Preview 2 grants. No environment, directory or network inheritance.
use crate::{Error, Result};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{FsPerms, WasiCtx, WasiCtxBuilder, WasiCtxView};

#[derive(Clone, Debug)]
pub struct Directory {
    pub host: PathBuf,
    pub guest: String,
    pub writable: bool,
}

/// Address authority for one TCP operation family. Connecting and listening
/// have independent policies; granting either does not grant the other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpAccess {
    /// Permit any IP address and port, subject to the host operating system.
    /// This does not enable DNS or UDP, or bypass host network restrictions.
    Unrestricted,
    /// Permit only these socket addresses. An empty list denies the operation.
    /// For binding, port zero permits an OS-selected port on that exact IP;
    /// it does not grant other IPs or arbitrary explicitly selected ports.
    Addresses(Vec<SocketAddr>),
}

impl Default for TcpAccess {
    /// Framework defaults grant no addresses. Applications may explicitly
    /// select unrestricted access when assembling their own product defaults.
    fn default() -> Self {
        Self::Addresses(Vec::new())
    }
}

impl TcpAccess {
    fn enabled(&self) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Addresses(addresses) => !addresses.is_empty(),
        }
    }

    fn allows(&self, address: SocketAddr) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Addresses(addresses) => addresses.contains(&address),
        }
    }

    /// Wasmtime checks the actual local address again before listening. A
    /// granted bind to port zero has already acquired its OS-selected port,
    /// so that listener must still be accepted for the same IP address.
    fn allows_listener(&self, address: SocketAddr) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::Addresses(addresses) => addresses.iter().any(|grant| {
                grant.ip() == address.ip() && (grant.port() == 0 || grant.port() == address.port())
            }),
        }
    }
}

/// Enabling WASI provides clocks/random and closed/discarded stdio by default.
/// Additional authority is supplied explicitly; DNS and UDP remain disabled.
#[derive(Clone, Debug, Default)]
pub struct Wasi {
    pub environment: BTreeMap<String, String>,
    pub arguments: Vec<String>,
    pub directories: Vec<Directory>,
    pub stdout: bool,
    pub stderr: bool,
    /// Remote destinations for outgoing TCP connections.
    pub tcp_connect: TcpAccess,
    /// Local addresses for TCP bind/listen, including acceptance by listeners.
    pub tcp_bind: TcpAccess,
}
pub(crate) struct State {
    ctx: WasiCtx,
    table: ResourceTable,
}
impl State {
    pub fn new(grants: &Wasi) -> Result<Self> {
        let mut builder = WasiCtxBuilder::new();
        builder.envs(&grants.environment.iter().collect::<Vec<_>>());
        builder.args(&grants.arguments);
        if grants.stdout {
            builder.inherit_stdout();
        }
        if grants.stderr {
            builder.inherit_stderr();
        }
        let mut names = std::collections::BTreeSet::new();
        for dir in &grants.directories {
            if dir.guest.is_empty() || !names.insert(&dir.guest) {
                return Err(Error::InvalidInput("duplicate or empty WASI mount".into()));
            }
            let permissions = if dir.writable {
                FsPerms::ReadWrite
            } else {
                FsPerms::ReadOnly
            };
            builder
                .preopened_dir(&dir.host, &dir.guest, permissions)
                .map_err(|e| Error::Load(format!("WASI directory {}: {e}", dir.host.display())))?;
        }
        builder.allow_ip_name_lookup(false).allow_udp(false);
        builder.allow_tcp(grants.tcp_connect.enabled() || grants.tcp_bind.enabled());
        let connect = grants.tcp_connect.clone();
        let bind = grants.tcp_bind.clone();
        builder.socket_addr_check(move |address, usage| {
            use wasmtime_wasi::sockets::SocketAddrUse;
            let allowed = match usage {
                SocketAddrUse::TcpConnect => connect.allows(address),
                // Connecting requires an ephemeral local socket. Permission
                // to listen remains separate, checked against tcp_bind below.
                SocketAddrUse::TcpBind => {
                    bind.allows(address)
                        || (connect.enabled()
                            && address.ip().is_unspecified()
                            && address.port() == 0)
                }
                SocketAddrUse::TcpListen => bind.allows_listener(address),
                SocketAddrUse::TcpAccept => bind.enabled(),
                _ => false,
            };
            Box::pin(async move { allowed })
        });
        Ok(Self {
            ctx: builder.build(),
            table: ResourceTable::new(),
        })
    }
    pub fn view(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}
