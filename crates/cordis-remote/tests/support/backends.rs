use super::*;

/// Select only implementations supported by this host. The TypeScript and
/// branch and fault peers do not require building the POSIX C++ process host.
#[derive(Clone, Copy, Debug)]
pub(super) enum Backend {
    TypeScript,
    #[cfg(unix)]
    Cpp,
}

impl Backend {
    pub(super) const ALL: &[Self] = &[
        Self::TypeScript,
        #[cfg(unix)]
        Self::Cpp,
    ];

    #[cfg(feature = "loader")]
    pub(super) fn is_typescript(self) -> bool {
        matches!(self, Self::TypeScript)
    }

    pub(super) fn spec(self, example: &str) -> ProcessSpec {
        self.program(
            &format!("examples/{example}.ts"),
            &format!("cordis_{example}"),
        )
    }

    pub(super) fn effect_fixture(self) -> ProcessSpec {
        self.program("test/effect-host.ts", "cordis_effects_test")
    }

    fn program(self, script: &str, _cpp: &str) -> ProcessSpec {
        match self {
            Self::TypeScript => {
                let mut spec = ProcessSpec::new("node");
                spec.args.push(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../sdk/typescript")
                        .join(script)
                        .display()
                        .to_string(),
                );
                spec
            }
            #[cfg(unix)]
            Self::Cpp => ProcessSpec::new(binary().with_file_name(_cpp)),
        }
    }

    #[cfg(feature = "loader")]
    pub(super) fn rolling_service(self) -> Service<RemoteService> {
        match self {
            Self::TypeScript => calculator(),
            #[cfg(unix)]
            Self::Cpp => test_calculator(),
        }
    }
}

pub(super) fn python() -> std::ffi::OsString {
    std::env::var_os("CORDIS_TEST_PYTHON")
        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into())
}

pub(super) fn python_spec() -> ProcessSpec {
    let mut spec = ProcessSpec::new(python());
    spec.args.push(cordis_test_contracts::PYTHON_RUNNER.into());
    spec
}
