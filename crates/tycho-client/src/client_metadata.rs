//! Generic client-metadata carried to the Tycho server in a dedicated header.
//!
//! This is the single source of truth for the header name and serialization so the RPC and
//! WebSocket paths can never drift. The map is deliberately untyped: `tycho-client` never learns
//! what the keys mean — consumers supply their own vocabulary.

use std::collections::BTreeMap;

use thiserror::Error;

/// Header name carrying serialized client metadata. Lowercase so it can be used with
/// `HeaderName::from_static`.
pub const CLIENT_METADATA_HEADER: &str = "x-tycho-client-metadata";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClientMetadataError {
    #[error("invalid client metadata key: {0:?}")]
    InvalidKey(String),
    #[error("invalid client metadata value: {0:?}")]
    InvalidValue(String),
}

/// Serializes client metadata into the `X-Tycho-Client-Metadata` header value.
///
/// Entries are emitted in key order as `key=value; key=value`. Returns `Ok(None)` for an empty
/// map, meaning no header should be sent (back-compatible default). Keys must be non-empty and
/// match `[A-Za-z0-9_.-]`; values must be non-empty visible ASCII excluding `;` and `=`. These
/// rules are stricter than `HeaderValue::from_str`, so any accepted output is always a valid
/// header value and the RPC path can never fail on serialized input.
pub fn serialize_client_metadata(
    meta: &BTreeMap<String, String>,
) -> Result<Option<String>, ClientMetadataError> {
    if meta.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(meta.len());
    for (key, value) in meta {
        if !is_valid_key(key) {
            return Err(ClientMetadataError::InvalidKey(key.clone()));
        }
        if !is_valid_value(value) {
            return Err(ClientMetadataError::InvalidValue(value.clone()));
        }
        parts.push(format!("{key}={value}"));
    }
    Ok(Some(parts.join("; ")))
}

fn is_valid_key(key: &str) -> bool {
    !key.is_empty() &&
        key.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn is_valid_value(value: &str) -> bool {
    !value.is_empty() &&
        value
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b';' && b != b'=')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn empty_map_yields_no_header() {
        assert_eq!(serialize_client_metadata(&BTreeMap::new()), Ok(None));
    }

    #[test]
    fn serializes_in_deterministic_key_order() {
        let meta = map(&[("preset", "best"), ("fynd_version", "0.57.0")]);
        assert_eq!(
            serialize_client_metadata(&meta),
            Ok(Some("fynd_version=0.57.0; preset=best".to_string()))
        );
    }

    #[test]
    fn rejects_invalid_keys() {
        for bad in ["", "has space", "semi;colon", "eq=uals", "unicod\u{00e9}"] {
            let meta = map(&[(bad, "v")]);
            assert!(
                matches!(
                    serialize_client_metadata(&meta),
                    Err(ClientMetadataError::InvalidKey(_))
                ),
                "expected InvalidKey for {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        for bad in ["", "has space", "semi;colon", "eq=uals", "ctrl\u{0007}", "unicod\u{00e9}"] {
            let meta = map(&[("k", bad)]);
            assert!(
                matches!(
                    serialize_client_metadata(&meta),
                    Err(ClientMetadataError::InvalidValue(_))
                ),
                "expected InvalidValue for {bad:?}"
            );
        }
    }

    #[test]
    fn accepted_output_is_a_valid_header_value() {
        let meta = map(&[("fynd_version", "0.57.0"), ("preset", "best")]);
        let serialized = serialize_client_metadata(&meta)
            .unwrap()
            .unwrap();
        assert!(reqwest::header::HeaderValue::from_str(&serialized).is_ok());
    }
}
