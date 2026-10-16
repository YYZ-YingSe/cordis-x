#!/usr/bin/env python3
"""Install Rust in the selected prefix without changing the user's rustup defaults."""
from common import main


def install(setup):
    cargo = setup.prefix / "cargo"
    setup.set(CARGO_HOME=cargo, RUSTUP_HOME=setup.prefix / "rustup", RUSTUP_TOOLCHAIN=setup.versions["rust"])
    setup.path(cargo / "bin")
    rustup = cargo / "bin" / ("rustup" + setup.exe)
    if not rustup.exists():
        downloaded = setup.download(f"rustup-{setup.host}")
        # The executable extension is required by CreateProcess on Windows.
        installer = setup.prefix / ("rustup-init" + setup.exe)
        import shutil
        shutil.copyfile(downloaded, installer)
        installer.chmod(0o755)
        setup.run(installer, "-y", "--no-modify-path", "--profile", "minimal", "--default-toolchain", "none")
    setup.run(rustup, "toolchain", "install", setup.versions["rust"], "--profile", "minimal",
              "--component", "rustfmt,clippy", "--target", "wasm32-unknown-unknown", "--no-self-update")


if __name__ == "__main__":
    main("rust")
