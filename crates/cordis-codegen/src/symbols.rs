//! Language spelling is separate from wire identity. Collisions are errors.
use crate::{Diagnostic, Result};
use heck::{ToSnakeCase, ToUpperCamelCase};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub enum Case {
    Type,
    Member,
}
pub fn ident(name: &str, case: Case) -> String {
    let s = match case {
        Case::Type => name.to_upper_camel_case(),
        Case::Member => name.to_snake_case(),
    };
    let mut result = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str(&format!(
                "{}{:x}",
                if matches!(case, Case::Type) { "U" } else { "u" },
                c as u32
            ));
        }
    }
    if result.is_empty() || result.starts_with(|c: char| c.is_ascii_digit()) || reserved(&result) {
        result = match case {
            Case::Type => format!("Cordis{result}"),
            Case::Member => format!("cordis_{result}"),
        };
    }
    result
}
fn reserved(s: &str) -> bool {
    // Union is deliberate: identical public spellings across the initial backends.
    matches!(
        s,
        "Self"
            | "self"
            | "super"
            | "crate"
            | "type"
            | "match"
            | "ref"
            | "move"
            | "async"
            | "await"
            | "dyn"
            | "use"
            | "mod"
            | "fn"
            | "pub"
            | "impl"
            | "trait"
            | "loop"
            | "class"
            | "struct"
            | "enum"
            | "union"
            | "template"
            | "namespace"
            | "typename"
            | "operator"
            | "delete"
            | "new"
            | "virtual"
            | "override"
            | "final"
            | "public"
            | "private"
            | "protected"
            | "const"
            | "static"
            | "auto"
            | "volatile"
            | "mutable"
            | "return"
            | "if"
            | "else"
            | "for"
            | "while"
            | "do"
            | "switch"
            | "case"
            | "default"
            | "break"
            | "continue"
            | "try"
            | "catch"
            | "throw"
            | "throws"
            | "this"
            | "true"
            | "false"
            | "null"
            | "nullptr"
            | "void"
            | "bool"
            | "int"
            | "long"
            | "short"
            | "char"
            | "float"
            | "double"
            | "signed"
            | "unsigned"
            | "sizeof"
            | "alignof"
            | "alignas"
            | "co_await"
            | "co_return"
            | "co_yield"
            | "export"
            | "import"
            | "function"
            | "let"
            | "var"
            | "interface"
            | "extends"
            | "implements"
            | "yield"
            | "package"
            | "in"
            | "of"
            | "instanceof"
            | "typeof"
            | "with"
            | "debugger"
            | "arguments"
            | "eval"
            | "constructor"
            | "prototype"
            | "__proto__"
            | "as"
            | "where"
            | "unsafe"
            | "extern"
            | "mut"
            | "abstract"
            | "become"
            | "box"
            | "macro"
            | "priv"
            | "unsized"
            | "gen"
            | "assert"
            | "and"
            | "and_eq"
            | "asm"
            | "bitand"
            | "bitor"
            | "char8_t"
            | "char16_t"
            | "char32_t"
            | "compl"
            | "concept"
            | "consteval"
            | "constexpr"
            | "constinit"
            | "const_cast"
            | "decltype"
            | "dynamic_cast"
            | "explicit"
            | "friend"
            | "goto"
            | "inline"
            | "noexcept"
            | "not"
            | "not_eq"
            | "or"
            | "or_eq"
            | "register"
            | "reinterpret_cast"
            | "requires"
            | "static_assert"
            | "static_cast"
            | "thread_local"
            | "typeid"
            | "typedef"
            | "using"
            | "wchar_t"
            | "xor"
            | "xor_eq"
            | "satisfies"
    )
}
#[derive(Default)]
pub struct Scope(BTreeMap<String, String>);
impl Scope {
    pub fn reserve(&mut self, symbol: &str) {
        self.0.insert(symbol.into(), "generated SDK member".into());
    }
    pub fn insert(&mut self, wire: &str, case: Case, path: &str) -> Result<String> {
        let symbol = ident(wire, case);
        if let Some(previous) = self.0.insert(symbol.clone(), wire.into()) {
            return Err(Diagnostic::new(
                path,
                format!("{wire:?} and {previous:?} both map to {symbol:?}"),
            ));
        }
        Ok(symbol)
    }
}
pub fn json(s: &str) -> String {
    serde_json::to_string(s).expect("string serialization")
}
pub fn cpp(s: &str) -> String {
    // Octal escapes have a fixed width; UTF-8 bytes and NUL survive exactly.
    let mut out = String::from("\"");
    for b in s.bytes() {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            32..=126 => out.push(b as char),
            _ => out.push_str(&format!("\\{b:03o}")),
        }
    }
    out.push('"');
    out
}
