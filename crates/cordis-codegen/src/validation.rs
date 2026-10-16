//! Generate borrowed, allocation-free field checks beside standard prost data.
//! Field layout and codecs remain owned by prost-build; tags join its AST to the
//! already validated descriptor, rather than guessing Rust names from the IDL.
use crate::{Diagnostic, Module, Result, backend::Plan};
use cordis_contract::protobuf::{flag, option, semantics};
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor, Value};
use quote::quote;
use std::collections::BTreeMap;

fn metadata(attributes: &[syn::Attribute]) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for attribute in attributes.iter().filter(|a| a.path().is_ident("prost")) {
        attribute
            .parse_nested_meta(|meta| {
                if meta.input.peek(syn::Token![=]) {
                    let value: syn::LitStr = meta.value()?.parse()?;
                    if let Some(name) = meta.path.get_ident() {
                        result.insert(name.to_string(), value.value());
                    }
                }
                Ok(())
            })
            .map_err(|e| Diagnostic::new("Rust validation metadata", e))?;
    }
    Ok(result)
}

fn checks(field: &FieldDescriptor) -> proc_macro2::TokenStream {
    let path = field.full_name();
    let mut output = quote! {};
    if let Some(Value::U32(n)) = option(&field.options(), "cordis.schema.exact_size") {
        let size = if matches!(field.kind(), Kind::String) && !field.is_list() && !field.is_map() {
            quote!(value.chars().count())
        } else {
            quote!(value.len())
        };
        let n = n as usize;
        output.extend(quote! {
            if #size != #n { return Err(format!("{}: fixed size mismatch", #path)); }
        });
    }
    for (annotation, below) in [("minimum", true), ("maximum", false)] {
        if let Some(Value::I64(bound)) =
            option(&field.options(), &format!("cordis.schema.{annotation}"))
        {
            let bound = bound as i128;
            let invalid = if below {
                quote!((*value as i128) < #bound)
            } else {
                quote!((*value as i128) > #bound)
            };
            output.extend(quote! {
                if #invalid { return Err(format!("{}: integer outside declared range", #path)); }
            });
        }
    }
    let kind = if field.is_map() {
        let Kind::Message(entry) = field.kind() else {
            unreachable!()
        };
        entry.get_field(2).unwrap().kind()
    } else {
        field.kind()
    };
    if let Kind::Enum(enumeration) = kind {
        let values: Vec<_> = enumeration.values().map(|v| v.number()).collect();
        let check = quote! {
            if ![#(#values),*].contains(number) {
                return Err(format!("{}: undeclared enum value", #path));
            }
        };
        output.extend(if field.is_map() {
            quote! { for number in value.values() { #check } }
        } else if field.is_list() {
            quote! { for number in value { #check } }
        } else {
            quote! { let number = value; #check }
        });
    }
    output
}

pub fn generate(module: &Module, package: &str, items: &mut Vec<syn::Item>) -> Result<()> {
    let descriptors: BTreeMap<_, _> = Plan::new(module)?
        .messages
        .into_iter()
        .map(|m| (m.rust, module.message(&m.name)))
        .collect();
    let package = if package == "_" { "" } else { package };
    let prefix = format!(
        "data::{}",
        prost_build::Module::from_protobuf_package_name(package)
            .parts()
            .collect::<Vec<_>>()
            .join("::")
    );
    let prefix = prefix.trim_end_matches("::");
    let mut oneofs = BTreeMap::new();
    collect_oneofs(items, prefix, &descriptors, &mut oneofs)?;
    visit(items, prefix, &descriptors, &oneofs)
}

fn collect_oneofs(
    items: &[syn::Item],
    prefix: &str,
    descriptors: &BTreeMap<String, MessageDescriptor>,
    oneofs: &mut BTreeMap<String, MessageDescriptor>,
) -> Result<()> {
    for item in items {
        match item {
            syn::Item::Struct(s) => {
                if let Some(desc) = descriptors.get(&format!("{prefix}::{}", s.ident)) {
                    for field in &s.fields {
                        if let Some(name) = metadata(&field.attrs)?.get("oneof") {
                            oneofs.insert(format!("{prefix}::{name}"), desc.clone());
                        }
                    }
                }
            }
            syn::Item::Mod(m) => {
                if let Some((_, items)) = &m.content {
                    collect_oneofs(
                        items,
                        &format!("{prefix}::{}", m.ident),
                        descriptors,
                        oneofs,
                    )?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn visit(
    items: &mut Vec<syn::Item>,
    prefix: &str,
    descriptors: &BTreeMap<String, MessageDescriptor>,
    oneofs: &BTreeMap<String, MessageDescriptor>,
) -> Result<()> {
    let mut extra = Vec::new();
    for item in items.iter_mut() {
        let tokens = match item {
            syn::Item::Struct(s) => {
                let name = &s.ident;
                let descriptor = descriptors.get(&format!("{prefix}::{name}"));
                if descriptor.is_none()
                    && !matches!(
                        format!("{prefix}::{name}").as_str(),
                        "data::cordis::data::Lease" | "data::cordis::data::Child"
                    )
                {
                    return Err(Diagnostic::new(
                        format!("{prefix}::{name}"),
                        "generated message has no validation descriptor",
                    ));
                }
                let mut fields = Vec::new();
                for field in &s.fields {
                    let member = field.ident.as_ref().unwrap();
                    let attrs = metadata(&field.attrs)?;
                    let mut constraints = quote! {};
                    if let Some(desc) = descriptor {
                        if let Some(tag) = attrs.get("tag") {
                            let f = desc
                                .get_field(
                                    tag.parse().map_err(|e| Diagnostic::new("field tag", e))?,
                                )
                                .ok_or_else(|| {
                                    Diagnostic::new(
                                        desc.full_name(),
                                        "prost field absent in descriptor",
                                    )
                                })?;
                            let check = checks(&f);
                            let path = f.full_name();
                            if flag(&f.options(), "cordis.schema.required") {
                                constraints.extend(quote! {
                                    if self.#member.is_none() { return Err(format!("{}: required field missing", #path)); }
                                });
                            }
                            if !check.is_empty() {
                                constraints.extend(if f.supports_presence() {
                                    quote! { if let Some(value) = &self.#member { #check } }
                                } else {
                                    quote! { { let value = &self.#member; #check } }
                                });
                            }
                        } else if let Some(tags) = attrs.get("tags") {
                            let first: u32 = tags
                                .split(',')
                                .next()
                                .unwrap()
                                .trim()
                                .parse()
                                .map_err(|e| Diagnostic::new("oneof tag", e))?;
                            let oneof = desc.get_field(first).unwrap().containing_oneof().unwrap();
                            if semantics(desc) != 0
                                || desc.full_name() == "google.protobuf.Value"
                                || flag(&oneof.options(), "cordis.schema.selected")
                            {
                                let path = oneof.full_name();
                                constraints.extend(quote! {
                                    if self.#member.is_none() { return Err(format!("{}: no branch selected", #path)); }
                                });
                            }
                        }
                    }
                    fields.push(quote! {
                        cordis_sdk_core::typed::Walk::walk(&self.#member, d + 1, b)?;
                        #constraints
                    });
                }
                quote! {
                    impl cordis_sdk_core::typed::Walk for #name {
                        fn walk(&self, d: usize, b: &mut cordis_sdk_core::typed::Budget) -> ::std::result::Result<(), ::std::string::String> {
                            b.take(d)?; #(#fields)* Ok(())
                        }
                    }
                }
            }
            syn::Item::Enum(e) => {
                let name = &e.ident;
                let descriptor = oneofs.get(&format!("{prefix}::{name}"));
                let body = if e
                    .variants
                    .iter()
                    .all(|v| matches!(v.fields, syn::Fields::Unit))
                {
                    quote! { b.take(d) }
                } else {
                    let mut branches = Vec::new();
                    for v in &e.variants {
                        let variant = &v.ident;
                        let mut check = quote! {};
                        if let Some(desc) = descriptor {
                            let attrs = metadata(&v.attrs)?;
                            let tag = attrs["tag"]
                                .parse()
                                .map_err(|e| Diagnostic::new("variant tag", e))?;
                            check = checks(&desc.get_field(tag).unwrap());
                        }
                        branches.push(quote! {
                            Self::#variant(value) => {
                                cordis_sdk_core::typed::Walk::walk(value, d, b)?;
                                #check
                                Ok(())
                            }
                        });
                    }
                    quote! { match self { #(#branches),* } }
                };
                quote! {
                    impl cordis_sdk_core::typed::Walk for #name {
                        fn walk(&self, d: usize, b: &mut cordis_sdk_core::typed::Budget) -> ::std::result::Result<(), ::std::string::String> { #body }
                    }
                }
            }
            syn::Item::Mod(m) => {
                if let Some((_, items)) = &mut m.content {
                    visit(
                        items,
                        &format!("{prefix}::{}", m.ident),
                        descriptors,
                        oneofs,
                    )?;
                }
                continue;
            }
            _ => continue,
        };
        extra.push(syn::parse2(tokens).map_err(|e| Diagnostic::new("Rust validation", e))?);
    }
    items.extend(extra);
    Ok(())
}
