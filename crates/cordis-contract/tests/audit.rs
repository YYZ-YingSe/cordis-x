use cordis_contract::{MessageContract, Validation};

#[test]
fn audit_checks_original_map_entries_and_overwritten_oneofs() {
    let shape = MessageContract::json_value();
    // Value.struct_value -> Struct.fields -> entry(key="k", value=null, field15=120).
    let map = [42, 11, 10, 9, 10, 1, b'k', 18, 2, 8, 0, 120, 120];
    // Value.struct_value(unknown field15), then overwrite the oneof with null.
    let overwritten = [42, 2, 120, 120, 8, 0];
    for bytes in [&map[..], &overwritten[..]] {
        for policy in [Validation::Contract, Validation::Checked] {
            shape.with_validation(policy).parse_binary(bytes).unwrap();
        }
        assert!(
            shape
                .with_validation(Validation::Audit)
                .parse_binary(bytes)
                .unwrap_err()
                .contains("undeclared protobuf field")
        );
    }
    // The same declared fields with no hidden bytes remain valid in Audit.
    shape
        .with_validation(Validation::Audit)
        .parse_binary(&[42, 9, 10, 7, 10, 1, b'k', 18, 2, 8, 0])
        .unwrap();
}
