//! Explicit build tools. Resolving tools never installs or changes their environment.
use crate::{Diagnostic, Result};
use std::{
    io,
    path::{Path, PathBuf},
    process::{Command, Output},
};

/// Keep UTF-8 arguments out of the Windows executable's narrow argv conversion.
/// protoc accepts one literal argument per line in an argument file.
pub(crate) fn protoc_output(command: &Command, temporary: &Path) -> io::Result<Output> {
    let mut arguments = String::new();
    for argument in command.get_args() {
        let argument = argument.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "protoc arguments must be UTF-8",
            )
        })?;
        if argument.contains(['\r', '\n']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "protoc arguments cannot contain line breaks",
            ));
        }
        arguments.push_str(argument);
        arguments.push('\n');
    }
    std::fs::write(temporary.join("protoc.args"), arguments)?;
    let program = Path::new(command.get_program());
    let program = if program.components().count() > 1 {
        dunce::canonicalize(program)?
    } else {
        program.to_owned()
    };
    let mut invocation = Command::new(program);
    for (key, value) in command.get_envs() {
        match value {
            Some(value) => invocation.env(key, value),
            None => invocation.env_remove(key),
        };
    }
    // CreateProcess receives this working directory as UTF-16. The argument
    // file's ASCII name also works when the temporary directory is non-ASCII.
    invocation
        .current_dir(temporary)
        .arg("@protoc.args")
        .output()
}

#[derive(Clone, Debug)]
pub struct GeneratorTools {
    pub protoc: PathBuf,
    pub protoc_gen_es: PathBuf,
}

impl Default for GeneratorTools {
    fn default() -> Self {
        Self {
            protoc: std::env::var_os("PROTOC")
                .map(PathBuf::from)
                .unwrap_or_else(|| "protoc".into()),
            protoc_gen_es: std::env::var_os("CORDIS_PROTOC_GEN_ES")
                .map(PathBuf::from)
                .unwrap_or_else(|| "protoc-gen-es".into()),
        }
    }
}

impl GeneratorTools {
    pub(crate) fn es_plugin(&self) -> Result<PathBuf> {
        let path = &self.protoc_gen_es;
        if path.components().count() > 1 || path.is_absolute() {
            return dunce::canonicalize(path)
                .map_err(|e| Diagnostic::new(path.display().to_string(), e));
        }
        for directory in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
            let candidate = directory.join(path);
            #[cfg(windows)]
            for extension in ["exe", "cmd", "bat"] {
                let candidate = candidate.with_extension(extension);
                if candidate.is_file() {
                    return dunce::canonicalize(candidate)
                        .map_err(|e| Diagnostic::new("protoc-gen-es", e));
                }
            }
            if candidate.is_file() {
                return dunce::canonicalize(candidate)
                    .map_err(|e| Diagnostic::new("protoc-gen-es", e));
            }
        }
        Err(Diagnostic::new(
            "protoc-gen-es",
            format!(
                "{} not found on PATH; provide --protoc-gen-es or CORDIS_PROTOC_GEN_ES",
                path.display()
            ),
        ))
    }

    pub(crate) fn configure_es(&self, command: &mut Command, temporary: &Path) -> Result<()> {
        let plugin = self.es_plugin()?;
        if cfg!(windows)
            && plugin.extension().is_some_and(|ext| {
                ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat")
            })
        {
            // protoc's explicit --plugin mapping uses CreateProcess directly.
            // Its PATH lookup uses cmd.exe, which is required for npm scripts.
            let directory = temporary.join("es-plugin");
            std::fs::create_dir_all(&directory)
                .map_err(|e| Diagnostic::new("protoc-gen-es launcher", e))?;
            let path = dunce::simplified(&plugin);
            std::fs::write(
                directory.join("protoc-gen-es.cmd"),
                "@echo off\r\n@setlocal DisableDelayedExpansion\r\n@\"%CORDIS_PROTOC_ES_SCRIPT%\"\r\n",
            )
            .map_err(|e| Diagnostic::new("protoc-gen-es launcher", e))?;
            let current = std::env::var_os("PATH").unwrap_or_default();
            let paths = std::env::join_paths(
                std::iter::once(directory).chain(std::env::split_paths(&current)),
            )
            .map_err(|e| Diagnostic::new("protoc-gen-es PATH", e))?;
            // Pass Unicode paths through the Windows environment, not batch text.
            command
                .env("PATH", paths)
                .env("CORDIS_PROTOC_ES_SCRIPT", path);
        } else {
            command.arg(format!("--plugin=protoc-gen-es={}", plugin.display()));
        }
        Ok(())
    }
}
