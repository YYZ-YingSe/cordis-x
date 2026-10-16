mod support {
    pub mod backends;
}
use support::backends::compare;

#[tokio::test]
async fn native_and_abi_have_the_same_lifecycle_trace() {
    tokio::time::timeout(std::time::Duration::from_secs(120), compare(None))
        .await
        .unwrap();
}
#[tokio::test]
async fn native_abi_and_ipc_have_the_same_lifecycle_trace() {
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        compare(Some(cordis_remote::Endpoint::Ipc)),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn native_abi_and_http_have_the_same_lifecycle_trace() {
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        compare(Some(cordis_remote::Endpoint::Http)),
    )
    .await
    .unwrap();
}
