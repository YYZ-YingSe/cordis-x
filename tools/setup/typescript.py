#!/usr/bin/env python3
"""Install Node and the repository's lockfile-selected npm tools."""
from common import ROOT, main


def install(setup):
    directory = setup.archive(f"node-{setup.host}", "node-" + setup.versions["node"])
    binary = directory if setup.windows else directory / "bin"
    npm = directory / ("node_modules/npm/bin/npm-cli.js" if setup.windows else "lib/node_modules/npm/bin/npm-cli.js")
    setup.path(binary)
    setup.run(binary / ("node" + setup.exe), npm, "ci", "--ignore-scripts", cwd=ROOT)
    setup.path(ROOT / "node_modules/.bin")
    setup.set(CORDIS_PROTOC_GEN_ES=ROOT / "node_modules/.bin" / ("protoc-gen-es.cmd" if setup.windows else "protoc-gen-es"))


if __name__ == "__main__":
    main("typescript")
