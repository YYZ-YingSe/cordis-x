use super::{host::Host, *};
use cordis_wire::{CONTENT_TYPE, Incoming, MAX_MESSAGE};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request, Response,
    body::{Bytes, Incoming as HttpBody},
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::watch;

async fn dispatch<P: Plugin>(
    request: Request<HttpBody>,
    host: Rc<Host<P>>,
) -> Result<Response<Full<Bytes>>> {
    let (parts, body) = request.into_parts();
    if parts.method != hyper::Method::POST
        || parts.uri.path() != "/cordis/call"
        || parts.uri.query().is_some()
    {
        return Err(refused("http", "unknown route"));
    }
    let header = |name: &str| -> Result<&str> {
        if parts.headers.get_all(name).iter().count() != 1 {
            return Err(refused("http", "duplicate or missing header"));
        }
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| refused("http", "invalid header"))
    };
    if header("content-type")? != CONTENT_TYPE || parts.headers.contains_key("transfer-encoding") {
        return Err(refused("http", "unsupported encoding"));
    }
    let length = header("content-length")?
        .parse::<usize>()
        .map_err(|e| refused("http", e))?;
    if length > MAX_MESSAGE {
        return Err(refused("http", "body too large"));
    }
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        Limited::new(body, MAX_MESSAGE).collect(),
    )
    .await
    .map_err(unknown)?
    .map_err(unknown)?
    .to_bytes();
    if bytes.len() != length {
        return Err(refused("http", "body length mismatch"));
    }
    let Incoming::Request(request) =
        cordis_wire::decode_with(bytes, host.channel.carrier_validation()).map_err(unknown)?
    else {
        return Err(refused("http", "expected request frame"));
    };
    if !matches!(
        request.command.as_str(),
        "call" | "resource_acquire" | "resource_call" | "resource_release"
    ) {
        return Err(refused("http", "command requires session endpoint"));
    }
    let response = host.dispatch(request).await?;
    let status = if response.is_success() { 200 } else { 400 };
    let bytes = response.encode().map_err(unknown)?;
    Response::builder()
        .status(status)
        .header("content-type", CONTENT_TYPE)
        .header("content-length", bytes.len())
        .body(Full::new(bytes))
        .map_err(unknown)
}
// Header/body I/O and idle/output waits are bounded independently. An admitted
// business future keeps the connection alive until its own runtime finishes it.
struct Active(watch::Sender<usize>);
impl Active {
    fn new(sender: watch::Sender<usize>) -> Self {
        sender.send_modify(|n| *n += 1);
        Self(sender)
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n -= 1);
    }
}
pub async fn serve<P: Plugin>(listener: tokio::net::TcpListener, host: Rc<Host<P>>) {
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(32));
    let mut jobs = tokio::task::JoinSet::new();
    while let Ok((socket, _)) = listener.accept().await {
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let host = host.clone();
        jobs.spawn_local(async move {
            let _permit = permit;
            let (activity, changed) = watch::channel(0usize);
            let service = hyper::service::service_fn(move |request| {
                let host = host.clone();
                let activity = Active::new(activity.clone());
                async move {
                    let _activity = activity;
                    Ok::<_, Infallible>(match dispatch(request, host).await {
                        Ok(response) => response,
                        Err(_) => Response::builder()
                            .status(400)
                            .header("connection", "close")
                            .body(Full::new(Bytes::new()))
                            .expect("fixed response"),
                    })
                }
            });
            connection(socket, service, changed).await;
        });
        while jobs.try_join_next().is_some() {}
    }
}

async fn connection<S>(
    socket: tokio::net::TcpStream,
    service: S,
    mut changed: watch::Receiver<usize>,
) where
    S: hyper::service::Service<
            Request<HttpBody>,
            Response = Response<Full<Bytes>>,
            Error = Infallible,
        >,
{
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .keep_alive(true)
        .max_headers(64)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(30));
    let connection = builder.serve_connection(TokioIo::new(socket), service);
    tokio::pin!(connection);
    loop {
        let idle = *changed.borrow_and_update() == 0;
        tokio::select! {
            biased;
            result = changed.changed() => { if result.is_err() { break; } }
            _ = &mut connection => break,
            _ = tokio::time::sleep(Duration::from_secs(30)), if idle => {
                // Polling the connection can admit a request in this
                // same select turn. Recheck before dropping its owner.
                if *changed.borrow() == 0 { break; }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    async fn request(socket: &mut BufReader<tokio::net::TcpStream>, path: &str) {
        socket
            .get_mut()
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut line = String::new();
        socket.read_line(&mut line).await.unwrap();
        assert!(line.starts_with("HTTP/1.1 200"), "{line}");
        loop {
            line.clear();
            assert!(socket.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        let mut body = [0; 2];
        socket.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"ok");
    }
    #[tokio::test]
    async fn reused_connection_does_not_expire_an_admitted_business_future() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (socket, _) = listener.accept().await.unwrap();
                let entered = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(tokio::sync::Notify::new());
                let (sender, receiver) = watch::channel(0);
                let service = hyper::service::service_fn({
                    let entered = entered.clone();
                    let release = release.clone();
                    move |request: Request<HttpBody>| {
                        let activity = Active::new(sender.clone());
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            let _activity = activity;
                            if request.uri().path() == "/hold" {
                                entered.notify_one();
                                release.notified().await;
                            }
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                        }
                    }
                });
                let serving = tokio::task::spawn_local(connection(socket, service, receiver));
                let mut client = BufReader::new(client);
                request(&mut client, "/first").await;
                let waiting = tokio::task::spawn_local(async move {
                    request(&mut client, "/hold").await;
                    client
                });
                entered.notified().await;
                tokio::time::sleep(Duration::from_secs(31)).await;
                assert!(
                    !waiting.is_finished(),
                    "connection age ended an admitted call"
                );
                release.notify_one();
                let mut client = tokio::time::timeout(Duration::from_secs(2), waiting)
                    .await
                    .unwrap()
                    .unwrap();
                request(&mut client, "/after").await;
                drop(client);
                tokio::time::timeout(Duration::from_secs(2), serving)
                    .await
                    .unwrap()
                    .unwrap();
            })
            .await;
    }
}
