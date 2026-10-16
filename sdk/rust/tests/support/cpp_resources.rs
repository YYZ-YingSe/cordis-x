//! Real C++ artifacts shared by the Rust/process and Rust/ABI resource suites.
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

pub fn build() -> &'static Path {
    static BUILD: OnceLock<PathBuf> = OnceLock::new();
    BUILD.get_or_init(|| {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let build = root.join("target/cpp-resources");
        let mut targets = vec![
            "--build".into(),
            build.clone().into_os_string(),
            "--config".into(),
            "Debug".into(),
            "--target".into(),
            "cordis_resource_counter".into(),
            "cordis_resource_client".into(),
            "cordis_resource_probe_client".into(),
        ];
        if cfg!(unix) {
            targets.push("cordis_resources_process".into());
        }
        targets.extend(["-j".into(), "4".into()]);
        for args in [
            vec![
                "-S".into(),
                root.join("sdk/cpp").into_os_string(),
                "-B".into(),
                build.clone().into_os_string(),
                "-DCMAKE_BUILD_TYPE=Debug".into(),
                "-DCORDIS_BUILD_EXAMPLES=ON".into(),
                "-DBUILD_TESTING=ON".into(),
                format!(
                    "-DCORDIS_BUILD_PROCESS={}",
                    if cfg!(unix) { "ON" } else { "OFF" }
                )
                .into(),
                // Multi-config generators (including MSVC) use the same artifact paths.
                format!("-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG={}", build.display()).into(),
                format!("-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG={}", build.display()).into(),
            ],
            targets,
        ] {
            let output = Command::new("cmake").args(args).output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        build
    })
}
