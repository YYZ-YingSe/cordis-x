use cordis_loader::{Configuration, ConfigurationLimits, Entry, json};

#[test]
fn deployment_budget_is_independent_of_one_business_message() {
    let config = Configuration {
        entries: (0..100)
            .map(|n| {
                Entry::new(
                    format!("plugin-{n}"),
                    "factory",
                    json!({"data":"x".repeat(16_384)}),
                )
            })
            .collect(),
    };
    let input = serde_json::to_string(&config).unwrap();
    assert!(input.len() > 1024 * 1024);
    assert_eq!(Configuration::from_json(&input).unwrap(), config);
    let limits = ConfigurationLimits {
        entries: 99,
        ..Default::default()
    };
    assert!(Configuration::from_json_with_limits(&input, &limits).is_err());
    let limits = ConfigurationLimits {
        bytes: input.len() - 1,
        ..Default::default()
    };
    assert!(Configuration::from_json_with_limits(&input, &limits).is_err());
    let limits = ConfigurationLimits {
        nodes: 1,
        ..Default::default()
    };
    assert!(Configuration::from_json_with_limits(&input, &limits).is_err());
}
