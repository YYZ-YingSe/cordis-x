/// Export a portable Plugin as the managed-plugin WIT world.
#[macro_export]
macro_rules! export {
    ($plugin:ty) => {
        #[allow(unsafe_code)]
        mod cordis_exports {
            use super::*;
            use $crate::bindings;
            struct Adapter;
            std::thread_local! {
                static INSTANCE: $crate::__private::Instance<$plugin> = Default::default();
            }
            impl bindings::exports::cordis::plugin::managed_service::Guest for Adapter {
                fn describe() -> bindings::exports::cordis::plugin::managed_service::Manifest {
                    $crate::__private::describe::<$plugin>()
                }
                async fn call(
                    context: String,
                    _cleanup: bool,
                    command: String,
                    args: bindings::cordis::plugin::types::Payload,
                ) -> std::result::Result<
                    bindings::cordis::plugin::types::Payload,
                    bindings::cordis::plugin::types::Failure,
                > {
                    $crate::__private::call::<$plugin>(&INSTANCE, context, command, args).await
                }
            }
            #[cfg(target_arch = "wasm32")]
            bindings::export_bindings!(Adapter with_types_in bindings);
        }
    };
}
