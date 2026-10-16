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
/// Enabling WASI provides clocks/random and closed/discarded stdio by default.
/// Additional authority is supplied explicitly; DNS and UDP remain disabled.
#[derive(Clone, Debug, Default)]
pub struct Wasi {
    pub environment: BTreeMap<String, String>,
    pub arguments: Vec<String>,
    pub directories: Vec<Directory>,
    pub stdout: bool,
    pub stderr: bool,
    pub tcp_connect: Vec<SocketAddr>,
    pub tcp_bind: Vec<SocketAddr>,
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
        builder.allow_tcp(!grants.tcp_connect.is_empty() || !grants.tcp_bind.is_empty());
        let connect = grants.tcp_connect.clone();
        let bind = grants.tcp_bind.clone();
        builder.socket_addr_check(move |address, usage| {
            use wasmtime_wasi::sockets::SocketAddrUse;
            let allowed = match usage {
                SocketAddrUse::TcpConnect => connect.contains(&address),
                // Connecting requires an ephemeral local socket. Permission
                // to listen remains separate, checked against tcp_bind below.
                SocketAddrUse::TcpBind => {
                    bind.contains(&address)
                        || (!connect.is_empty()
                            && address.ip().is_unspecified()
                            && address.port() == 0)
                }
                SocketAddrUse::TcpListen => bind.iter().any(|grant| {
                    grant.ip() == address.ip()
                        && (grant.port() == 0 || grant.port() == address.port())
                }),
                SocketAddrUse::TcpAccept => !bind.is_empty(),
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
