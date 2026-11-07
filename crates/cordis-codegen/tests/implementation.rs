//! Public clients and separately bound providers share one checked interface.
use cordis_sdk_core::{self as sdk, Context, LocalFuture, Plugin};
use cordis_test_contracts::{
    provider_local as local, provider_remote as remote, public_service as public,
};
use std::rc::Rc;
struct Local(String);
impl local::EchoServer for Local {
    fn create(config: local::data::fixture::provider::Local) -> sdk::Result<Self> {
        Ok(Self(config.directory))
    }
    fn get(
        self: Rc<Self>,
        _: Context,
        _: local::data::fixture::public_api::Value,
    ) -> LocalFuture<local::data::fixture::public_api::Value> {
        Box::pin(async move {
            Ok(local::data::fixture::public_api::Value {
                text: self.0.clone(),
            })
        })
    }
}
struct Remote(String);
impl remote::EchoServer for Remote {
    fn create(config: remote::data::fixture::provider::Remote) -> sdk::Result<Self> {
        Ok(Self(config.endpoint))
    }
    fn get(
        self: Rc<Self>,
        _: Context,
        _: remote::data::fixture::public_api::Value,
    ) -> LocalFuture<remote::data::fixture::public_api::Value> {
        Box::pin(async move {
            Ok(remote::data::fixture::public_api::Value {
                text: self.0.clone(),
            })
        })
    }
}
#[test]
fn distinct_constructors_share_the_public_surface() {
    assert_eq!(
        public::EchoClient::contract(),
        local::EchoClient::contract()
    );
    assert_eq!(
        sdk::Descriptor::new(public::EchoClient::interface()).unwrap(),
        sdk::Descriptor::new(local::EchoClient::interface()).unwrap()
    );
    assert_eq!(
        sdk::Descriptor::new(public::EchoClient::interface()).unwrap(),
        sdk::Descriptor::new(remote::EchoClient::interface()).unwrap()
    );
    local::EchoExport::<Local>::create(sdk::json!({"directory":"/data"})).unwrap();
    remote::EchoExport::<Remote>::create(
        sdk::json!({"endpoint":"https://objects.invalid","tokenName":"key"}),
    )
    .unwrap();
    assert!(local::EchoExport::<Local>::create(sdk::json!({"endpoint":"wrong shape"})).is_err());
}
