//! WASI operations receive validated messages; WASI still enforces host grants.
use cordis_portable_example::context::data::{
    component_example::{FilePath, FileWrite, Port},
    google::protobuf::{BytesValue, Empty},
};
use cordis_sdk_core::{Result, error, typed::violation};
use wasi::{
    filesystem::{
        preopens,
        types::{Descriptor, DescriptorFlags, OpenFlags, PathFlags},
    },
    io::streams::{InputStream, OutputStream},
    sockets::{
        instance_network,
        network::{ErrorCode, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress},
        tcp::TcpSocket,
        tcp_create_socket,
    },
};
fn wasi_error(e: impl std::fmt::Debug) -> cordis_sdk_core::Failure {
    error("wasi", format!("{e:?}"))
}
fn open(path: &str, flags: OpenFlags, access: DescriptorFlags) -> Result<Descriptor> {
    let dirs = preopens::get_directories();
    let (dir, _) = dirs
        .first()
        .ok_or_else(|| error("no_directory", "no directory granted"))?;
    dir.open_at(PathFlags::empty(), path, flags, access)
        .map_err(wasi_error)
}
/// A bounded binary preview, at most 4096 bytes from offset zero.
pub fn read(args: FilePath) -> Result<BytesValue> {
    let file = open(
        &args.path.unwrap(),
        OpenFlags::empty(),
        DescriptorFlags::READ,
    )?;
    let (value, _) = file.read(4096, 0).map_err(wasi_error)?;
    Ok(BytesValue {
        value: value.into(),
    })
}
pub fn write(args: FileWrite) -> Result<Empty> {
    let file = open(
        &args.path.unwrap(),
        OpenFlags::CREATE | OpenFlags::TRUNCATE,
        DescriptorFlags::WRITE,
    )?;
    let data = args.data.unwrap();
    let mut offset = 0;
    while offset < data.len() {
        let written = file
            .write(&data[offset..], offset as u64)
            .map_err(wasi_error)?;
        if written == 0 || written > (data.len() - offset) as u64 {
            return Err(error("wasi", "invalid write progress"));
        }
        offset += written as usize;
    }
    Ok(Empty {})
}
pub fn listen() -> Result<Empty> {
    let socket = tcp_create_socket::create_tcp_socket(IpAddressFamily::Ipv4).map_err(wasi_error)?;
    socket
        .start_bind(
            &instance_network::instance_network(),
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                port: 0,
                address: (0, 0, 0, 0),
            }),
        )
        .map_err(wasi_error)?;
    loop {
        match socket.finish_bind() {
            Ok(()) => break,
            Err(ErrorCode::WouldBlock) => socket.subscribe().block(),
            Err(e) => return Err(wasi_error(e)),
        }
    }
    socket.start_listen().map_err(wasi_error)?;
    loop {
        match socket.finish_listen() {
            Ok(()) => break,
            Err(ErrorCode::WouldBlock) => socket.subscribe().block(),
            Err(e) => return Err(wasi_error(e)),
        }
    }
    Ok(Empty {})
}
// Rust drops struct fields in declaration order: streams must precede their
// parent socket, including when a connect-only caller discards the connection.
pub struct Connection {
    input: InputStream,
    output: OutputStream,
    _socket: TcpSocket,
}
pub fn connect(args: Port) -> Result<Connection> {
    let port =
        u16::try_from(args.port.unwrap()).map_err(|_| violation("port validation was bypassed"))?;
    let socket = tcp_create_socket::create_tcp_socket(IpAddressFamily::Ipv4).map_err(wasi_error)?;
    socket
        .start_connect(
            &instance_network::instance_network(),
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                port,
                address: (127, 0, 0, 1),
            }),
        )
        .map_err(wasi_error)?;
    let (input, output) = loop {
        match socket.finish_connect() {
            Ok(streams) => break streams,
            Err(ErrorCode::WouldBlock) => socket.subscribe().block(),
            Err(e) => return Err(wasi_error(e)),
        }
    };
    Ok(Connection {
        input,
        output,
        _socket: socket,
    })
}
pub fn exchange(args: Port) -> Result<BytesValue> {
    let connection = connect(args)?;
    connection
        .output
        .blocking_write_and_flush(b"hello")
        .map_err(wasi_error)?;
    let mut value = Vec::new();
    while value.len() < 5 {
        value.extend(
            connection
                .input
                .blocking_read((5 - value.len()) as u64)
                .map_err(wasi_error)?,
        );
    }
    Ok(BytesValue {
        value: value.into(),
    })
}
