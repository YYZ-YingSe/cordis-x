#!/usr/bin/env bash
# Explicitly install OS prerequisites. All remaining tools use setup.py --prefix.
set -euo pipefail
case "$(uname -s)" in
  Darwin)
    if ! xcode-select -p >/dev/null 2>&1; then
      xcode-select --install
      echo 'Finish installing the Apple command line tools, then rerun this script.'
      exit 1
    fi
    if ! command -v brew >/dev/null 2>&1; then
      echo 'Install Homebrew from https://brew.sh, then rerun this script.' >&2
      exit 1
    fi
    brew install python@3.12 git
    echo "Next: $(brew --prefix python@3.12)/bin/python3.12 tools/setup/setup.py --prefix <tool-directory>"
    ;;
  Linux)
    if ! command -v apt-get >/dev/null 2>&1; then
      echo 'This recipe supports Ubuntu 24.04+ / Debian 13+. Install Python 3.12+, venv, a C++20 compiler, make, git, patch and CA certificates with your package manager.' >&2
      exit 1
    fi
    elevate=()
    if [ "$(id -u)" -ne 0 ]; then elevate=(sudo); fi
    "${elevate[@]}" apt-get update
    "${elevate[@]}" apt-get install -y build-essential python3 python3-venv python3-pip git patch ca-certificates curl pkg-config
    echo 'Next: python3 tools/setup/setup.py --prefix <tool-directory>'
    ;;
  *) echo 'On Windows, run tools/setup/system.ps1 in PowerShell.' >&2; exit 1 ;;
esac
