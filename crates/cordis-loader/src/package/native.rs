use super::{FileKind, PackageManifest, err};
use crate::Result;
use goblin::Object;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Static metadata, parsed with Goblin, never by executing a candidate (ldd).
#[derive(Clone, Debug)]
pub struct NativeImage {
    pub kind: FileKind,
    pub identity: Option<String>,
    pub imports: Vec<String>,
    pub search_paths: Vec<String>,
}
impl NativeImage {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let (os, arch, kind, identity, imports, search_paths) = match Object::parse(bytes)
            .map_err(err)?
        {
            Object::Mach(goblin::mach::Mach::Binary(m)) => {
                if m.header.flags & goblin::mach::header::MH_TWOLEVEL == 0 {
                    return Err(err("Mach-O packages require two-level namespace binding"));
                }
                let arch = match m.header.cputype {
                    0x0100000c => "aarch64",
                    0x01000007 => "x86_64",
                    _ => "unsupported",
                };
                let kind = match m.header.filetype {
                    6 => FileKind::Library,
                    2 => FileKind::Executable,
                    _ => return Err(err("unsupported Mach-O file kind")),
                };
                (
                    "macos",
                    arch,
                    kind,
                    m.name.map(str::to_owned),
                    m.libs.into_iter().skip(1).map(str::to_owned).collect(),
                    m.rpaths.into_iter().map(str::to_owned).collect(),
                )
            }
            Object::Elf(e) => {
                let arch = match e.header.e_machine {
                    183 => "aarch64",
                    62 => "x86_64",
                    _ => "unsupported",
                };
                let kind =
                    if e.header.e_type == goblin::elf::header::ET_EXEC || e.interpreter.is_some() {
                        FileKind::Executable
                    } else if e.header.e_type == goblin::elf::header::ET_DYN {
                        FileKind::Library
                    } else {
                        return Err(err("package member is not an ELF executable/library"));
                    };
                let paths = if e.runpaths.is_empty() {
                    e.rpaths
                } else {
                    e.runpaths
                };
                (
                    "linux",
                    arch,
                    kind,
                    e.soname.map(str::to_owned),
                    e.libraries.into_iter().map(str::to_owned).collect(),
                    paths
                        .into_iter()
                        .flat_map(|p| p.split(':').map(str::to_owned))
                        .collect(),
                )
            }
            Object::PE(p) => {
                if p.header
                    .optional_header
                    .as_ref()
                    .is_some_and(|h| h.data_directories.get_delay_import_descriptor().is_some())
                {
                    return Err(err(
                        "delay-loaded PE dependencies must be made explicit at link time",
                    ));
                }
                let arch = match p.header.coff_header.machine {
                    0xaa64 => "aarch64",
                    0x8664 => "x86_64",
                    _ => "unsupported",
                };
                (
                    "windows",
                    arch,
                    if p.is_lib {
                        FileKind::Library
                    } else {
                        FileKind::Executable
                    },
                    p.name.map(|n| n.to_ascii_lowercase()),
                    p.libraries
                        .into_iter()
                        .map(|n| n.to_ascii_lowercase())
                        .collect(),
                    Vec::new(),
                )
            }
            _ => return Err(err("package requires a supported native binary")),
        };
        if os != std::env::consts::OS || arch != std::env::consts::ARCH {
            return Err(err(
                "native binary architecture/platform does not match host",
            ));
        }
        Ok(Self {
            kind,
            identity,
            imports,
            search_paths,
        })
    }
}
pub(super) fn private_name(import: &str) -> &str {
    import.strip_prefix("@loader_path/").unwrap_or(import)
}

pub(super) fn verify_links(
    manifest: &PackageManifest,
    images: &BTreeMap<String, NativeImage>,
) -> Result<()> {
    let mut libraries = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for (path, image) in images {
        if image.kind != FileKind::Library {
            continue;
        }
        let name = Path::new(path).file_name().unwrap().to_str().unwrap();
        if libraries.insert(name.to_ascii_lowercase(), path).is_some() {
            return Err(err(
                "native library basenames must be unique within a package",
            ));
        }
        if let Some(id) = &image.identity
            && !identities.insert(id)
        {
            return Err(err("duplicate native link identity"));
        }
    }
    let mut used_externals = BTreeSet::new();
    for (path, image) in images {
        for import in &image.imports {
            if let Some(private) = libraries.get(&private_name(import).to_ascii_lowercase()) {
                if std::env::consts::OS != "windows"
                    && Path::new(private).file_name().unwrap() != private_name(import)
                {
                    return Err(err("private import spelling differs from package filename"));
                }
                if Path::new(path).parent() != Path::new(private).parent() {
                    return Err(err(
                        "private native dependencies must be siblings of their importer",
                    ));
                }
                match std::env::consts::OS {
                    "macos" if !import.starts_with("@loader_path/") => {
                        return Err(err("private Mach-O imports must use @loader_path"));
                    }
                    "linux" => {
                        if import.contains('/')
                            || image.search_paths.first().map(String::as_str) != Some("$ORIGIN")
                        {
                            return Err(err(
                                "private ELF imports require a basename and first search path $ORIGIN",
                            ));
                        }
                        if images[*private].identity.as_deref() != Some(import) {
                            return Err(err(
                                "private ELF library SONAME must equal its package filename",
                            ));
                        }
                    }
                    _ => {}
                }
            } else if manifest.host_dependencies.contains_key(import) {
                used_externals.insert(import);
            } else {
                return Err(err(format!("unresolved native import in {path}: {import}")));
            }
        }
    }
    // Script packages may declare an interpreter/runtime even without native
    // images. Native packages declare precisely their static external imports.
    if !images.is_empty()
        && manifest
            .host_dependencies
            .keys()
            .any(|k| !used_externals.contains(k))
    {
        return Err(err(
            "native manifest declares an unused external dependency",
        ));
    }
    Ok(())
}
pub(super) fn system_dependencies() -> BTreeMap<String, String> {
    let names: &[&str] = match std::env::consts::OS {
        "macos" => &[
            "/usr/lib/libSystem.B.dylib",
            "/usr/lib/libc++.1.dylib",
            "/usr/lib/libobjc.A.dylib",
        ],
        "linux" => &[
            "libc.so.6",
            "libm.so.6",
            "libpthread.so.0",
            "libdl.so.2",
            "librt.so.1",
            "libstdc++.so.6",
            "libgcc_s.so.1",
            "ld-linux-x86-64.so.2",
            "ld-linux-aarch64.so.1",
            "libc.musl-x86_64.so.1",
            "libc.musl-aarch64.so.1",
        ],
        "windows" => &[
            "kernel32.dll",
            "user32.dll",
            "advapi32.dll",
            "ws2_32.dll",
            "bcrypt.dll",
            "ntdll.dll",
            "shell32.dll",
            "ole32.dll",
            "msvcrt.dll",
            "ucrtbase.dll",
            "ucrtbased.dll",
            "vcruntime140.dll",
            "vcruntime140d.dll",
            "vcruntime140_1.dll",
            "vcruntime140_1d.dll",
            "msvcp140.dll",
            "msvcp140d.dll",
            "api-ms-win-crt-runtime-l1-1-0.dll",
            "api-ms-win-crt-heap-l1-1-0.dll",
            "api-ms-win-crt-stdio-l1-1-0.dll",
            "api-ms-win-crt-string-l1-1-0.dll",
            "api-ms-win-crt-math-l1-1-0.dll",
            "api-ms-win-crt-convert-l1-1-0.dll",
            "api-ms-win-crt-locale-l1-1-0.dll",
            "api-ms-win-crt-filesystem-l1-1-0.dll",
            "api-ms-win-crt-time-l1-1-0.dll",
            "api-ms-win-crt-environment-l1-1-0.dll",
            "api-ms-win-crt-utility-l1-1-0.dll",
        ],
        _ => &[],
    };
    names
        .iter()
        .map(|n| ((*n).into(), "system".into()))
        .collect()
}
