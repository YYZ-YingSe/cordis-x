# Public contracts and implementation bindings

A service's `name`, `revision`, RPC messages, resources and controls define its public interface. Configuration and implementation dependencies are a separate construction declaration. A public service may omit `config`; the generated API then has typed clients and server interfaces but no constructible Rust `Export`, C++ `Model`, or TypeScript export factory.

A provider imports the public `.proto` and declares its private configuration messages in its own `.proto`. Supply an explicit JSON binding when generating that provider:

```json
{
  "example.objects.Objects": {
    "config": "example.local.Settings",
    "dependencies": {"authorization": "example.auth.Authorization"}
  }
}
```

```sh
cordis-codegen generate --input provider.proto --proto-path contracts \
  --implementation provider.json --out generated --languages rust,cpp,typescript
```

Every referenced service and message must exist in the descriptor input. The binding changes constructors and declared dependencies; it does not change the public service descriptor. Rust packages can map the shared DTO package with `--rust-package`, so providers and consumers use the same message types without copying handwritten declarations. No transport-specific bridge is needed.

A small, self-contained implementation can declare `config` and `dependencies` in its service option. This is an explicit combined declaration, with the same semantic model. A service cannot declare a constructor in both locations. Duplicate JSON keys, aliases, repeated dependency contracts, unknown names and conflicting declarations fail generation. Dependencies without a construction declaration are rejected.

Generated metadata contains `contract`, `interface`, `implementation` and `bindings`. `implementation` is either null or an object containing `config` and `dependencies`. Loaders selecting an executable export must require an implementation declaration; consumers need only the public descriptor. The generated client's `contract()` accessor supplies its identity in Rust, C++ and TypeScript.

Generation does not grant capabilities, select OS targets, weaken checks or duplicate the host's resource lifecycle. Generated dispatch still decodes according to the validation mode selected by its host binding.

Repository fixtures are generated through `tests/build.py` and `examples/build.py`. The public-service/local-provider/remote-provider fixtures cover different private configuration shapes, identity equality, actual client dispatch and conflicting declaration failures.
