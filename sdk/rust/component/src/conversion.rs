use crate::bindings::{cordis::plugin::types, exports::cordis::plugin::managed_service::Manifest};
use cordis_component::Payload;
use cordis_sdk_core::{
    Contract, Descriptor, ErrorInfo, Failure, Interface, Plugin, PluginManifest, Result, refused,
};

pub(crate) fn decode(
    value: types::Payload,
    validation: cordis_sdk_core::Validation,
) -> Result<Payload> {
    cordis_component::decode_with(value, validation).map_err(|e| refused("invalid_payload", e))
}
pub(crate) fn encode(value: &Payload) -> Result<types::Payload> {
    cordis_component::encode(value).map_err(|e| refused("invalid_payload", e))
}
impl From<types::ErrorInfo> for ErrorInfo {
    fn from(value: types::ErrorInfo) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}
impl From<ErrorInfo> for types::ErrorInfo {
    fn from(value: ErrorInfo) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}
impl From<types::Failure> for Failure {
    fn from(value: types::Failure) -> Self {
        match value {
            types::Failure::Refused(info) => Self::Refused(info.into()),
            types::Failure::Rejected(info) => Self::Rejected(info.into()),
            types::Failure::Unknown(info) => Self::Unknown(info.into()),
        }
    }
}
impl From<Failure> for types::Failure {
    fn from(value: Failure) -> Self {
        match value {
            Failure::Refused(info) => Self::Refused(info.into()),
            Failure::Rejected(info) => Self::Rejected(info.into()),
            Failure::Unknown(info) => Self::Unknown(info.into()),
        }
    }
}
fn described(value: Contract, interface: Interface) -> Result<types::Contract> {
    let descriptor = Descriptor::new(interface).map_err(|e| refused("invalid_contract", e))?;
    Ok(types::Contract {
        name: value.name,
        revision: value.revision,
        descriptor: encode(&cordis_sdk_core::contract::encode(&descriptor).into())?,
    })
}
fn manifest(
    value: PluginManifest,
    interface: Interface,
    mut dependencies: std::collections::BTreeMap<String, Interface>,
) -> Result<Manifest> {
    let mut requires = Vec::new();
    for required in value.requires {
        let interface = dependencies.remove(&required.name).ok_or_else(|| {
            refused(
                "invalid_contract",
                "missing or duplicate dependency descriptor",
            )
        })?;
        requires.push(described(required, interface)?);
    }
    if !dependencies.is_empty() {
        return Err(refused(
            "invalid_contract",
            "undeclared dependency descriptor",
        ));
    }
    Ok(Manifest {
        provides: described(value.provides, interface)?,
        requires,
    })
}
/// Build the current WIT manifest with mandatory structural descriptions.
pub fn describe<P: Plugin>() -> Manifest {
    manifest(P::manifest(), P::interface(), P::dependency_interfaces())
        .expect("valid plugin interface declarations")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_sdk_core::json;

    #[test]
    fn failure_classification_survives_component_boundary() {
        let info = ErrorInfo {
            code: "failed".into(),
            message: "preserve responsibility".into(),
        };
        for failure in [
            Failure::Rejected(info.clone()),
            Failure::Refused(info.clone()),
            Failure::Unknown(info),
        ] {
            assert_eq!(
                Failure::from(types::Failure::from(failure.clone())),
                failure
            );
        }
    }

    #[test]
    fn portable_manifest_maps_to_generated_contracts() {
        let declaration = PluginManifest {
            provides: Contract {
                name: "service".into(),
                revision: "2".into(),
            },
            requires: vec![Contract {
                name: "dependency".into(),
                revision: "1".into(),
            }],
        };
        let manifest = manifest(
            declaration,
            Interface::value_dispatch(true),
            std::collections::BTreeMap::from([(
                "dependency".into(),
                Interface::value_dispatch(true),
            )]),
        )
        .unwrap();
        assert_eq!(
            (
                manifest.provides.name.as_str(),
                manifest.provides.revision.as_str()
            ),
            ("service", "2")
        );
        assert_eq!(manifest.requires.len(), 1);
        assert_eq!(
            (
                manifest.requires[0].name.as_str(),
                manifest.requires[0].revision.as_str()
            ),
            ("dependency", "1")
        );
    }

    #[test]
    fn manifest_requires_one_valid_descriptor_per_dependency() {
        let declaration = || PluginManifest {
            provides: Contract {
                name: "provided".into(),
                revision: "1".into(),
            },
            requires: vec![Contract {
                name: "dependency".into(),
                revision: "1".into(),
            }],
        };
        let interface = Interface::value_dispatch(true);
        assert!(manifest(declaration(), interface.clone(), Default::default()).is_err());
        let mut dependencies =
            std::collections::BTreeMap::from([("dependency".into(), interface.clone())]);
        dependencies.insert("undeclared".into(), interface.clone());
        assert!(manifest(declaration(), interface.clone(), dependencies.clone()).is_err());
        dependencies.remove("undeclared");
        let mut duplicate = declaration();
        duplicate.requires.push(duplicate.requires[0].clone());
        assert!(manifest(duplicate, interface.clone(), dependencies.clone()).is_err());
        let mapped = manifest(declaration(), interface.clone(), dependencies).unwrap();
        let descriptor = Descriptor::new(interface).unwrap();
        for contract in [mapped.provides, mapped.requires.into_iter().next().unwrap()] {
            let value = decode(contract.descriptor, cordis_sdk_core::Validation::Audit)
                .unwrap()
                .control_only()
                .unwrap();
            assert_eq!(
                cordis_sdk_core::contract::decode(&value).unwrap(),
                descriptor
            );
        }
    }

    #[test]
    fn component_buffer_validation_and_body_presence() {
        let value =
            Payload::with_message(json!({"文字": [null,true,42,"hello"]}), vec![0, 255].into());
        assert_eq!(
            decode(
                encode(&value).unwrap(),
                cordis_sdk_core::Validation::Checked
            )
            .unwrap(),
            value
        );
        assert!(
            matches!(decode(vec![], cordis_sdk_core::Validation::Checked),Err(Failure::Refused(info)) if info.code=="invalid_payload")
        );
    }
}
