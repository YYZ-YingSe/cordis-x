fn main() {
    println!(
        "cargo:rustc-env=CORDIS_PACKAGE_TARGET={}",
        std::env::var("TARGET").unwrap()
    );
}
