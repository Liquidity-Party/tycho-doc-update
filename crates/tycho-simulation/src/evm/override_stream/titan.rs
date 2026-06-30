//! Concrete [`StateOverrideProvider`] backed by the Titan pAMM state-override stream.
//!
//! Connects to the Titan `pamm_quote_stream` WebSocket and tracks the latest per-block VM storage
//! overrides published for the known pAMM venues (e.g. FermiSwap), exposing them through the
//! generic [`StateOverrideProvider`] trait so the decoder can inject them into the matching
//! `EVMPoolState` instances before simulation. See
//! <https://docs.titanbuilder.xyz/propamms/takers>.
//!
//! All Titan/Fermi-specific knowledge (venue addresses, lane-timestamp resolution, endpoint URL)
//! lives only in this module; the rest of the override machinery is protocol-agnostic.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::primitives::{Address, U256};
use futures::StreamExt;
use serde_json::Value;
use tokio::{sync::watch, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::{OverrideSnapshot, StateOverrideProvider};

/// Storage overrides keyed by contract address, then by storage slot.
type VenueOverrides = HashMap<Address, HashMap<U256, U256>>;

/// Default Titan pAMM quote-stream WebSocket endpoint serving all known pAMM venues.
const TITAN_URL: &str = "wss://eu.rpc.titanbuilder.xyz/ws/pamm_quote_stream";

/// FermiSwap protocol system identifier as indexed by Tycho.
const FERMISWAP_PROTOCOL_SYSTEM: &str = "vm:fermiswap";
/// Kipseli protocol system identifier as indexed by Tycho.
const KIPSELI_PROTOCOL_SYSTEM: &str = "vm:kipseli";
/// bopAMM protocol system identifier as indexed by Tycho.
const BOPAMM_PROTOCOL_SYSTEM: &str = "vm:bopamm";

/// FermiSwap pAMM venue address on Titan's quote stream.
const FERMISWAP_VENUE: &str = "0xb1076fe3ab5e28005c7c323bac5ac06a680d452e";
/// Kipseli pAMM venue address on Titan's quote stream.
const KIPSELI_VENUE: &str = "0x5cdbe59400cc2efdcc2b54acca4a99fe00dd588c";
/// bopAMM pAMM venue address on Titan's quote stream.
const BOPAMM_VENUE: &str = "0x160141a205f5ddcf096ba3f48b7ed21eb52c62ea";

/// Returns the pAMM `protocol_system`s this provider knows how to serve.
fn known_pamm_protocols() -> &'static [&'static str] {
    &[FERMISWAP_PROTOCOL_SYSTEM, KIPSELI_PROTOCOL_SYSTEM, BOPAMM_PROTOCOL_SYSTEM]
}

/// Default override providers contributed by Titan, keyed by `protocol_system`.
///
/// Returns one shared [`TitanProvider`] mapped under every known pAMM `protocol_system` that is
/// both present in `registered_exchanges` and absent from `covered`. The provider is spawned only
/// when at least one such venue needs serving — so a caller who overrides every Titan venue opens
/// no connection. The single provider is shared across its protocols via `Arc`, not duplicated.
pub fn default_providers(
    registered_exchanges: &[String],
    covered: &HashSet<String>,
) -> HashMap<String, Arc<dyn StateOverrideProvider>> {
    let needed: Vec<&'static str> = known_pamm_protocols()
        .iter()
        .copied()
        .filter(|&protocol| {
            registered_exchanges
                .iter()
                .any(|e| e.as_str() == protocol) &&
                !covered.contains(protocol)
        })
        .collect();
    if needed.is_empty() {
        return HashMap::new();
    }
    let provider: Arc<dyn StateOverrideProvider> = Arc::new(TitanProvider::spawn(TITAN_URL.to_string()));
    needed
        .into_iter()
        .map(|protocol| (protocol.to_string(), provider.clone()))
        .collect()
}

/// A [`StateOverrideProvider`] backed by a single Titan pAMM WebSocket connection.
///
/// One connection serves all known pAMM venues; per-protocol snapshots are exposed through the
/// `watch` receivers held here. Cheap to clone.
pub struct TitanProvider {
    /// Latest resolved snapshot per served `protocol_system`, kept fresh by the background task.
    receivers: HashMap<String, watch::Receiver<OverrideSnapshot>>,
}

impl TitanProvider {
    /// Opens ONE WebSocket connection to Titan serving all known pAMM venues and spawns the
    /// background reconnect/parse task that keeps the per-protocol snapshots up to date.
    ///
    /// Returns immediately; the connection is established and maintained in the background, so a
    /// transient WebSocket failure never blocks or crashes the caller.
    pub fn spawn(url: String) -> Self {
        let mut receivers = HashMap::new();
        let mut senders = Vec::new();
        for &protocol in known_pamm_protocols() {
            let Some(venue) = Self::venue_for_protocol(protocol) else {
                continue;
            };
            let (tx, rx) = watch::channel(OverrideSnapshot::default());
            receivers.insert(protocol.to_string(), rx);
            senders.push((venue, tx));
        }
        tokio::spawn(Self::run(url, senders));
        Self { receivers }
    }

    /// Maintains the Titan WebSocket connection forever: parses each message into a per-venue
    /// [`OverrideSnapshot`] and publishes it on the matching channel, reconnecting with capped
    /// exponential backoff on any disconnect or error.
    async fn run(url: String, senders: Vec<(Address, watch::Sender<OverrideSnapshot>)>) {
        let mut attempt: u32 = 0;
        loop {
            match connect_async(url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!(%url, "Connected to Titan pAMM quote stream");
                    attempt = 0;
                    while let Some(message) = ws_stream.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                for (venue, sender) in &senders {
                                    match Self::parse_message(text.as_str(), venue) {
                                        Ok(Some(snapshot)) => {
                                            let _ = sender.send(snapshot);
                                        }
                                        Ok(None) => {}
                                        Err(e) => {
                                            warn!(%venue, error = %e, "Failed to parse Titan message");
                                        }
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                info!("Titan quote stream closed by server");
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                error!(error = %e, "Titan quote stream read error");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect to Titan quote stream");
                }
            }
            attempt = attempt.saturating_add(1);
            let backoff = Duration::from_secs(2u64.pow(attempt.min(5)));
            warn!(seconds = backoff.as_secs(), attempt, "Reconnecting to Titan quote stream");
            sleep(backoff).await;
        }
    }

    /// Maps a known pAMM `protocol_system` to its Titan venue address, or `None` if unknown.
    ///
    /// Titan/Fermi specifics (the venue address mapping) live only here.
    fn venue_for_protocol(protocol_system: &str) -> Option<Address> {
        let raw = match protocol_system {
            FERMISWAP_PROTOCOL_SYSTEM => FERMISWAP_VENUE,
            KIPSELI_PROTOCOL_SYSTEM => KIPSELI_VENUE,
            BOPAMM_PROTOCOL_SYSTEM => BOPAMM_VENUE,
            _ => return None,
        };
        raw.parse().ok()
    }

    /// Extracts a single venue's snapshot (`stateDiff` overrides + resolved block environment)
    /// from a Titan stream message.
    ///
    /// Returns `Ok(None)` if the venue is absent from the message. The block timestamp is resolved
    /// from the freshest lane update timestamp (see
    /// [`max_lane_timestamp`](Self::max_lane_timestamp)) so the pool's oracle-staleness guard
    /// sees the overridden prices as current.
    fn parse_message(text: &str, venue: &Address) -> Result<Option<OverrideSnapshot>, String> {
        let root: Value = serde_json::from_str(text).map_err(|e| format!("invalid JSON: {e}"))?;

        // Each message carries one top-level entry per venue address, alongside `slot`,
        // `blockNumber` and a wall-clock `timestamp`. Absent venue => nothing to apply.
        let venue_key = format!("{venue:#x}");
        let Some(venue_entry) = root.get(&venue_key) else {
            return Ok(None);
        };

        let block_number = root
            .get("blockNumber")
            .and_then(Value::as_u64);

        // `stateOverride` mirrors the eth_call State Override Set: account -> { stateDiff: slot ->
        // value, ... }. We only consume `stateDiff` storage; balance/nonce/code are ignored.
        let mut storage: VenueOverrides = HashMap::new();
        if let Some(state_override) = venue_entry
            .get("stateOverride")
            .and_then(Value::as_object)
        {
            for (account, account_state) in state_override {
                let address = account
                    .parse::<Address>()
                    .map_err(|e| format!("invalid account address {account}: {e}"))?;
                let Some(state_diff) = account_state
                    .get("stateDiff")
                    .and_then(Value::as_object)
                else {
                    continue;
                };
                let mut slots = HashMap::new();
                for (slot, value) in state_diff {
                    let slot = slot
                        .parse::<U256>()
                        .map_err(|e| format!("invalid storage slot {slot}: {e}"))?;
                    let value = value
                        .as_str()
                        .ok_or_else(|| format!("storage value for slot {slot} is not a string"))?
                        .parse::<U256>()
                        .map_err(|e| format!("invalid storage value: {e}"))?;
                    slots.insert(slot, value);
                }
                if !slots.is_empty() {
                    storage.insert(address, slots);
                }
            }
        }

        // Resolve the block timestamp from the freshest lane update, not the wall-clock
        // `timestamp` field, so the registry's oracle-staleness guard treats the streamed prices
        // as current.
        let block_timestamp = Self::max_lane_timestamp(&storage);

        Ok(Some(OverrideSnapshot { block_number, block_timestamp, storage }))
    }

    /// Returns the newest lane update timestamp (in seconds) across all overridden slots.
    ///
    /// FermiSwap registry lanes pack a uint32 update timestamp in the first 4 bytes of the slot
    /// value. This extracts that prefix from every overridden slot and returns the maximum value
    /// that looks like a plausible unix timestamp, or `None` if there are none. The result is used
    /// as the resolved `block_timestamp` so the registry's staleness guard treats the streamed
    /// oracle prices as current.
    fn max_lane_timestamp(overrides: &VenueOverrides) -> Option<u64> {
        // Lane slots pack a uint32 unix-seconds timestamp in the top 4 bytes (`value >> 224`).
        // Non-lane slots have other (usually zero) top bytes, so we keep only values inside a
        // plausible window to discard them.
        const MIN_PLAUSIBLE: u64 = 1_000_000_000; // ~2001-09
        const MAX_PLAUSIBLE: u64 = u32::MAX as u64; // uint32 ceiling (~2106)
        overrides
            .values()
            .flat_map(|slots| slots.values())
            .filter_map(|value| {
                let candidate = u64::try_from(*value >> 224).ok()?;
                (MIN_PLAUSIBLE..=MAX_PLAUSIBLE)
                    .contains(&candidate)
                    .then_some(candidate)
            })
            .max()
    }
}

impl StateOverrideProvider for TitanProvider {
    fn subscribe(&self, protocol_system: &str) -> Option<watch::Receiver<OverrideSnapshot>> {
        self.receivers
            .get(protocol_system)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The known-protocols list must contain the FermiSwap protocol so the builder auto-registers
    /// the provider.
    #[test]
    fn known_pamm_protocols_contains_fermiswap() {
        assert!(known_pamm_protocols().contains(&FERMISWAP_PROTOCOL_SYSTEM));
    }

    #[test]
    fn venue_for_protocol_resolves_all_known_venues() {
        let cases = [
            (FERMISWAP_PROTOCOL_SYSTEM, FERMISWAP_VENUE),
            (KIPSELI_PROTOCOL_SYSTEM, KIPSELI_VENUE),
            (BOPAMM_PROTOCOL_SYSTEM, BOPAMM_VENUE),
        ];
        for (protocol, expected) in cases {
            assert_eq!(
                TitanProvider::venue_for_protocol(protocol).expect("venue must resolve"),
                expected.parse::<Address>().unwrap(),
                "venue mismatch for {protocol}",
            );
        }
    }

    #[test]
    fn venue_for_protocol_returns_none_for_unknown() {
        assert!(TitanProvider::venue_for_protocol("vm:unknown").is_none());
    }

    /// Sample message from the Titan docs (<https://docs.titanbuilder.xyz/propamms/takers>).
    const SAMPLE_MESSAGE: &str = r#"{
        "slot": 14285824,
        "blockNumber": 25051224,
        "timestamp": 1778253913749564761,
        "0xb1076fe3ab5e28005c7c323bac5ac06a680d452e": {
            "stateOverride": {
                "0x1038c87766e36d1925889e6f26d10e0012d50fed": {
                    "balance": "0x0",
                    "nonce": "0x1",
                    "stateDiff": {
                        "0x156b1d71de08fed89d0fce38008e2b9d03a8998077e394b20597ef3d148f5ebc": "0x000000000000000000000000000000000000000000000000000000017e405801"
                    }
                }
            }
        }
    }"#;

    #[test]
    fn parse_message_extracts_venue_state_diff() {
        let venue = FERMISWAP_VENUE.parse::<Address>().unwrap();
        let snapshot = TitanProvider::parse_message(SAMPLE_MESSAGE, &venue)
            .expect("parse must succeed")
            .expect("venue is present");

        assert_eq!(snapshot.block_number, Some(25051224));

        let account = "0x1038c87766e36d1925889e6f26d10e0012d50fed"
            .parse::<Address>()
            .unwrap();
        let slot = "0x156b1d71de08fed89d0fce38008e2b9d03a8998077e394b20597ef3d148f5ebc"
            .parse::<U256>()
            .unwrap();
        let value = "0x000000000000000000000000000000000000000000000000000000017e405801"
            .parse::<U256>()
            .unwrap();
        assert_eq!(snapshot.storage.get(&account).and_then(|s| s.get(&slot)), Some(&value));
    }

    #[test]
    fn parse_message_returns_none_for_absent_venue() {
        let venue = KIPSELI_VENUE.parse::<Address>().unwrap();
        assert!(TitanProvider::parse_message(SAMPLE_MESSAGE, &venue)
            .unwrap()
            .is_none());
    }

    #[test]
    fn max_lane_timestamp_picks_newest_plausible_top_word() {
        let addr = FERMISWAP_VENUE.parse::<Address>().unwrap();
        // Two lane slots with the timestamp packed in the top 4 bytes, plus a non-lane slot
        // whose top bytes are zero (must be ignored).
        let older = U256::from(1_700_000_000u64) << 224;
        let newer = U256::from(1_800_000_000u64) << 224;
        let non_lane = U256::from(42u64);
        let overrides = HashMap::from([(
            addr,
            HashMap::from([
                (U256::from(0u64), older),
                (U256::from(1u64), newer),
                (U256::from(2u64), non_lane),
            ]),
        )]);
        assert_eq!(TitanProvider::max_lane_timestamp(&overrides), Some(1_800_000_000));
    }

    #[test]
    fn max_lane_timestamp_none_when_no_plausible_values() {
        let addr = FERMISWAP_VENUE.parse::<Address>().unwrap();
        let overrides = HashMap::from([(addr, HashMap::from([(U256::from(0u64), U256::from(7u64))]))]);
        assert!(TitanProvider::max_lane_timestamp(&overrides).is_none());
    }
}
