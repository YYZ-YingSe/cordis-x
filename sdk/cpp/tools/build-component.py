#!/usr/bin/env python3
"""Build consumer/Cordis code using externally prepared WASI tools and target libraries."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess


def executable(value, label):
    resolved = shutil.which(str(value)) if value else None
    if not resolved:
        raise SystemExit(f'{label}: executable {value!r} is unavailable; provide its path or prepare PATH. No tools are installed automatically.')
    return str(Path(resolved).resolve())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--core', action='store_true')
    parser.add_argument('--sdk-source', type=Path, default=Path(__file__).resolve().parents[1], help='Cordis C++ source directory; defaults to this script\'s SDK')
    parser.add_argument('--wasi-sdk', type=Path, default=os.environ.get('WASI_SDK_PATH'))
    parser.add_argument('--toolchain', type=Path, help='explicit CMake WASI toolchain file')
    parser.add_argument('--dependency-prefix', action='append', type=Path, default=[Path(p) for p in os.environ.get('CORDIS_WASM_PREFIX', '').split(os.pathsep) if p], help='externally built WASI Protobuf/Abseil prefix; repeat as needed')
    parser.add_argument('--wit-bindgen', default=os.environ.get('WIT_BINDGEN', 'wit-bindgen'))
    parser.add_argument('--protoc', default=os.environ.get('PROTOC', 'protoc'))
    parser.add_argument('--component-pack', default=os.environ.get('CORDIS_COMPONENT_PACK', 'cordis-component-pack'))
    parser.add_argument('--cmake', default='cmake')
    parser.add_argument('--proto', action='append', default=[], type=Path)
    parser.add_argument('--proto-path', action='append', default=[], type=Path)
    parser.add_argument('--include', action='append', default=[], type=Path, help='consumer generated/source include directories')
    parser.add_argument('--build-dir', type=Path)
    parser.add_argument('--jobs', type=int, default=4)
    args = parser.parse_args()
    toolchain = args.toolchain or (args.wasi_sdk / 'share/cmake/wasi-sdk-p1.cmake' if args.wasi_sdk else None)
    if toolchain is None or not toolchain.is_file():
        parser.error('provide --toolchain or --wasi-sdk / WASI_SDK_PATH pointing to an existing WASI environment')
    if not args.dependency_prefix:
        parser.error('provide --dependency-prefix or CORDIS_WASM_PREFIX for externally prepared WASI Protobuf/Abseil (host libraries cannot be used)')
    for prefix in args.dependency_prefix:
        if not prefix.is_dir():
            parser.error(f'target dependency prefix does not exist: {prefix}')
    if not (args.sdk_source / 'CMakeLists.txt').is_file():
        parser.error(f'Cordis C++ source SDK not found: {args.sdk_source}')
    if args.jobs <= 0:
        parser.error('--jobs must be positive')
    if not args.source.is_file():
        parser.error(f'source does not exist: {args.source}')
    cmake = executable(args.cmake, 'CMake')
    bindgen = executable(args.wit_bindgen, 'wit-bindgen')
    protoc = executable(args.protoc, 'host protoc') if args.proto else ''
    pack = executable(args.component_pack, 'cordis-component-pack') if not args.core else ''
    includes = [p.resolve() for p in args.proto_path]
    protos = []
    for proto in args.proto:
        candidate = proto if proto.is_file() else next((p / proto for p in includes if (p / proto).is_file()), None)
        if candidate is None:
            parser.error(f'proto not found in the supplied --proto-path directories: {proto}')
        protos.append(candidate.resolve())
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    build = (args.build_dir or output.parent / (output.stem + '-build')).resolve()
    prefixes = [p.resolve() for p in args.dependency_prefix]
    command = [cmake, '-S', str(Path(__file__).resolve().parent / 'component-project'), '-B', str(build),
        '-DCMAKE_BUILD_TYPE=MinSizeRel', f'-DCMAKE_TOOLCHAIN_FILE={toolchain.resolve()}',
        '-DCMAKE_PREFIX_PATH=' + ';'.join(map(str, prefixes)),
        '-DCMAKE_FIND_ROOT_PATH=' + ';'.join(map(str, prefixes)),
        f'-DCORDIS_SOURCE={args.source.resolve()}', f'-DCORDIS_OUTPUT={output}',
        f'-DCORDIS_SDK_SOURCE={args.sdk_source.resolve()}',
        f'-DCORDIS_CORE={"ON" if args.core else "OFF"}', '-DCORDIS_BUILD_EXAMPLES=OFF', '-DBUILD_TESTING=OFF',
        f'-DCORDIS_PROTOC={protoc}', f'-DCORDIS_WIT_BINDGEN={bindgen}', f'-DCORDIS_COMPONENT_PACK={pack}',
        '-DCORDIS_INCLUDE_DIRS=' + ';'.join(str(p.resolve()) for p in args.include),
        '-DCORDIS_PROTOS=' + ';'.join(map(str, protos)), '-DCORDIS_PROTO_PATHS=' + ';'.join(map(str, includes))]
    subprocess.run(command, check=True)
    subprocess.run([cmake, '--build', str(build), '--parallel', str(args.jobs)], check=True)


if __name__ == '__main__':
    main()
