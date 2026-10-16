//! Process entry for the counter business shared with the native SDK example.
use cordis_portable_example::resources::{CounterClient, CounterPlugin};
fn main() {
    let options = cordis_sdk_process::Options {
        config: cordis_sdk_core::json!({}),
        http: !std::env::args().any(|a| a == "--stdio-only"),
    };
    let result = if std::env::args().any(|a| a == "--client") {
        cordis_sdk_process::run::<CounterClient>(options)
    } else {
        cordis_sdk_process::run::<CounterPlugin>(options)
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
