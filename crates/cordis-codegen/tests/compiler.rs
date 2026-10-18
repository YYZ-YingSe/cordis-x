use cordis_codegen::{
    GeneratorTools,
    backend::{self, cpp::Cpp, rust::Rust, typescript::Typescript},
    compile,
    output::Files,
};
use std::path::PathBuf;
fn root() -> PathBuf {
    PathBuf::from(cordis_example_contracts::CONTRACT_SOURCES)
}
fn source(text: &str) -> cordis_codegen::Result<cordis_codegen::Module> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.proto");
    std::fs::write(&path, text).unwrap();
    compile(
        &path,
        &[dir.path().into(), root()],
        &GeneratorTools::default(),
    )
}
fn proto(body: &str) -> String {
    format!(
        r#"syntax="proto3";package test;import "cordis/options.proto";import "cordis/types.proto";{}
service Test {{option(cordis.schema.contract)={{name:"test.probe" revision:"1" config:"cordis.data.Unit"}};rpc Echo(Data)returns(Data);}}"#,
        body
    )
}

#[test]
fn nested_proto_paths_work_with_spaces_and_unicode() {
    let directory = tempfile::tempdir().unwrap();
    let include = directory.path().join("合同 sources");
    std::fs::create_dir_all(include.join("nested")).unwrap();
    let input = include.join("nested/test.proto");
    std::fs::write(&input, proto("message Data {}")).unwrap();
    let module = compile(&input, &[include], &GeneratorTools::default()).unwrap();
    assert!(module.services().contains_key("Test"));
    let output = backend::generate(&module, &[&Cpp], &GeneratorTools::default()).unwrap();
    assert!(
        output
            .iter()
            .any(|(name, _)| name == "cpp/proto/nested/test.pb.cc")
    );
}
#[test]
fn rejects_invalid_types_constraints_and_nonproductive_recursion() {
    for body in [
        "message Data { Missing value=1; }",
        "message Data { int32 value=1 [(cordis.schema.required)=true]; }",
        "message Data { int32 value=1 [(cordis.schema.exact_size)=3]; }",
        "message Data { Data next=1 [(cordis.schema.required)=true]; }",
        "message Data { repeated Data next=1 [(cordis.schema.exact_size)=1]; }",
        "message Data { map<string,Data> next=1 [(cordis.schema.exact_size)=1]; }",
        "message Data { cordis.data.Lease capability=1; }",
        "message Data { option(cordis.schema.semantics)=OPTION; bool value=1; }",
        "message Data { optional uint32 value=1 [(cordis.schema.minimum)=10,(cordis.schema.maximum)=2]; }",
    ] {
        assert!(source(&proto(body)).is_err(), "accepted {body}");
    }
}
#[test]
fn productive_recursion_and_nested_options_are_valid() {
    source(&proto(
        "message Data { Data next=1; repeated Data children=2; } ",
    ))
    .unwrap();
    let tests = PathBuf::from(cordis_test_contracts::CONTRACT_SOURCES);
    compile(
        &tests.join("profile.proto"),
        &[tests],
        &GeneratorTools::default(),
    )
    .unwrap();
}
#[test]
fn controls_have_distinct_roles_and_cannot_serialize_authority() {
    let valid = std::fs::read_to_string(root().join("workflows.proto")).unwrap();
    assert!(source(&valid).is_ok());
    for text in [
        valid.replace(
            "(cordis.schema.installs) = true",
            "(cordis.schema.installs) = false",
        ),
        valid.replace("returns (cordis.data.Child)", "returns (cordis.data.Unit)"),
        valid.replace(
            "(cordis.schema.event) =",
            "(cordis.schema.installs) = true; option (cordis.schema.event) =",
        ),
        valid.replace(
            "(cordis.schema.provision) =",
            "(cordis.schema.errors) = \"business\"; option (cordis.schema.provision) =",
        ),
        valid.replace(
            "name: \"example.tick\" revision: \"1\"",
            "name: \"example.tick\"",
        ),
        valid.replace("rpc TickEvent(Tick)", "rpc OtherEvent(cordis.data.Unit) returns (cordis.data.Int64) { option (cordis.schema.event) = {name: \"example.tick\" revision: \"1\"}; } rpc TickEvent(Tick)"),
        valid.replace(
            "message Tick {",
            "message Illegal { cordis.data.Child forged = 1; } message Tick {",
        ),
    ] {
        assert!(
            source(&text).is_err(),
            "accepted invalid control declaration"
        );
    }
}
#[test]
fn naming_and_dependencies_are_checked_before_emission() {
    let text=proto("message Data {}").replace("rpc Echo(Data)returns(Data);",r#"rpc A(Data)returns(Data){option(cordis.schema.wire_name)="foo-bar";}rpc B(Data)returns(Data){option(cordis.schema.wire_name)="foo_bar";}"#);
    let module = source(&text).unwrap();
    assert!(backend::generate(&module, &[&Rust], &GeneratorTools::default()).is_err());
    let text = proto("message Data {}").replace(
        r#"config:"cordis.data.Unit""#,
        r#"config:"cordis.data.Unit" dependencies:{alias:"missing" service:"test.Missing"}"#,
    );
    assert!(source(&text).is_err());
    let text = proto("message Data {} service Unbound { rpc Echo(Data) returns(Data); }").replace(
        r#"config:"cordis.data.Unit""#,
        r#"config:"cordis.data.Unit" dependencies:{alias:"unbound" service:"test.Unbound"}"#,
    );
    assert!(source(&text).is_err());
    let text = proto("message Data {}").replace(
        r#"config:"cordis.data.Unit""#,
        r#"config:"cordis.data.Unit" capabilities:"example" capabilities:"example""#,
    );
    assert!(source(&text).is_err());
}
#[test]
fn generators_are_deterministic_and_use_library_messages() {
    let module = compile(
        &root().join("examples.proto"),
        &[root()],
        &GeneratorTools::default(),
    )
    .unwrap();
    let ts = Typescript {
        sdk: "@cordis-x/sdk".into(),
    };
    let backends: [&dyn backend::Backend; 3] = [&Rust, &Cpp, &ts];
    let a = backend::generate(&module, &backends, &GeneratorTools::default()).unwrap();
    let b = backend::generate(&module, &backends, &GeneratorTools::default()).unwrap();
    assert_eq!(a.iter().collect::<Vec<_>>(), b.iter().collect::<Vec<_>>());
    assert!(a.iter().any(|(name, _)| name.ends_with("examples.pb.cc")));
    assert!(
        a.iter()
            .any(|(name, text)| name.ends_with("example.rs") && text.contains("::prost::Message"))
    );
    assert!(a.iter().any(|(name, _)| name.ends_with("examples_pb.ts")));
    assert!(a.iter().all(|(name, _)| {
        ["rust/", "cpp/", "typescript/", "resources/"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
    }));
}
#[test]
fn generated_outputs_are_owned_and_replaced_as_a_set() {
    let directory = std::env::current_dir().unwrap().join(format!(
        "../../target/codegen-output-test-{}",
        std::process::id()
    ));
    if directory.exists() {
        std::fs::remove_dir_all(&directory).unwrap();
    }
    let mut files = Files::default();
    assert!(files.add("../escape", "x".into()).is_err());
    files.add("nested/file", "first".into()).unwrap();
    files.write(&directory).unwrap();
    std::fs::write(directory.join("unmanaged"), "keep").unwrap();
    assert!(files.write(&directory).is_err());
    assert_eq!(
        std::fs::read_to_string(directory.join("unmanaged")).unwrap(),
        "keep"
    );
    std::fs::remove_file(directory.join("unmanaged")).unwrap();
    std::fs::write(directory.join("nested/file"), "drift").unwrap();
    let mut second = Files::default();
    second.add("replacement", "second".into()).unwrap();
    second.write(&directory).unwrap();
    assert!(!directory.join("nested/file").exists());
    assert_eq!(
        std::fs::read_to_string(directory.join("replacement")).unwrap(),
        "second"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn imported_contract_identity_is_independent_of_the_consumer_module() {
    let logger = compile(
        &root().join("fixtures/logger.proto"),
        &[root()],
        &GeneratorTools::default(),
    )
    .unwrap();
    let pricing = compile(
        &root().join("fixtures/pricing.proto"),
        &[root()],
        &GeneratorTools::default(),
    )
    .unwrap();
    let orders = compile(
        &root().join("fixtures/orders.proto"),
        &[root()],
        &GeneratorTools::default(),
    )
    .unwrap();
    assert_eq!(
        logger.services()["Logger"].descriptor,
        pricing.services()["Logger"].descriptor
    );
    assert_eq!(
        pricing.services()["Pricing"].descriptor,
        orders.services()["Pricing"].descriptor
    );
}

#[test]
fn rust_packages_reuse_shared_dtos_in_fields_and_bindings() {
    let mut module = source(&proto("message Data { cordis.data.Int64 value=1; }")).unwrap();
    module
        .rust_package("cordis.data", "::shared_dto::cordis::data")
        .unwrap();
    assert!(module.rust_package("cordis.data", "::other").is_err());
    assert!(module.rust_package("missing", "::other").is_err());
    assert!(module.rust_package("test", "::other<T>").is_err());
    let files = backend::generate(&module, &[&Rust], &GeneratorTools::default()).unwrap();
    let fields = files
        .iter()
        .find(|(path, _)| *path == "rust/proto/test.rs")
        .unwrap()
        .1;
    assert!(fields.contains("::shared_dto::cordis::data::Int64"));
    let bindings = files
        .iter()
        .find(|(path, _)| *path == "rust/mod.rs")
        .unwrap()
        .1;
    assert!(bindings.contains("::shared_dto::cordis::data::Unit"));
    assert!(!bindings.contains("impl sdk::typed::Codec for ::shared_dto"));
    assert!(
        !files
            .iter()
            .any(|(path, _)| path == "rust/proto/cordis.data.rs")
    );
}
