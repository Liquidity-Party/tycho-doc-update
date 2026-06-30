//! Generic, protocol-agnostic core for injecting live per-block VM state overrides into pools.
//!
//! Some protocols (e.g. pAMMs such as FermiSwap) rely on off-chain oracle prices that are not part
//! of the on-chain state Tycho indexes, and that change *within* a block. To simulate them
//! accurately, a side channel publishes per-block VM storage overrides and an overridden block
//! environment that must be reflected by the pool on every simulation — not just once per Tycho
//! block.
//!
//! This module defines the generic plumbing:
//! - [`OverrideSnapshot`] — the resolved overrides for one protocol at a point in time.
//! - [`StateOverrideProvider`] — a source that maintains a *live* [`watch`] channel of snapshots
//!   per protocol (kept fresh in the background, e.g. from a WebSocket).
//!
//! Providers are registered per `protocol_system` (see
//! [`ProtocolStreamBuilder::with_override_provider`](crate::evm::stream::ProtocolStreamBuilder::with_override_provider)).
//! A pool subscribes to the provider registered for its protocol at creation time and reads the
//! freshest snapshot at simulation time.
//!
//! It deliberately knows nothing about any specific venue or stream format; all such specifics live
//! in the concrete provider implementations (see [`titan`]).

pub mod titan;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy::primitives::{Address, U256};
use tokio::sync::watch;

/// Resolved per-block VM overrides for a single protocol at a point in time.
///
/// `block_number` and `block_timestamp` are already resolved by the provider (e.g. Titan computes
/// `block_timestamp` from lane timestamps) so this core stays protocol-agnostic. Either field may
/// be `None`, in which case the pool's existing block environment is left intact.
#[derive(Clone, Default, Debug)]
pub struct OverrideSnapshot {
    /// The L1 block number these overrides apply to, if known.
    pub block_number: Option<u64>,
    /// The block timestamp to inject into the pool's simulation environment, if known.
    pub block_timestamp: Option<u64>,
    /// Storage overrides keyed by contract address, then by storage slot.
    pub storage: HashMap<Address, HashMap<U256, U256>>,
}

/// Supplies a *live* stream of resolved per-block VM overrides for one or more protocols.
///
/// Implementations maintain their snapshots in the background (e.g. from a WebSocket stream) and
/// expose them through a [`watch`] channel per protocol. A single provider may serve several
/// protocols sharing one underlying connection; the same provider can be registered for each of
/// them (it is shared via `Arc`, not duplicated).
pub trait StateOverrideProvider: Send + Sync {
    /// Returns the live override channel for `protocol_system`, or `None` if unsupported.
    ///
    /// The returned [`watch::Receiver`] always reflects the latest snapshot; reading it
    /// ([`watch::Receiver::borrow`]) never blocks, so a pool may read it on every simulation.
    fn subscribe(&self, protocol_system: &str) -> Option<watch::Receiver<OverrideSnapshot>>;
}

/// The default registry of built-in override providers, keyed by `protocol_system`.
///
/// Given the `registered_exchanges` and the set of protocols already `covered` by explicit
/// consumer registrations, returns the providers that should serve the remaining protocols. This
/// is the single place that wires concrete providers (e.g. the Titan pAMM stream) into the
/// otherwise protocol-agnostic stream builder, keeping all venue-specific knowledge out of both the
/// builder and the generic override core. Protocols in `covered` are left to the consumer.
pub(crate) fn default_override_providers(
    registered_exchanges: &[String],
    covered: &HashSet<String>,
) -> HashMap<String, Arc<dyn StateOverrideProvider>> {
    let mut providers: HashMap<String, Arc<dyn StateOverrideProvider>> = HashMap::new();
    providers.extend(titan::default_providers(registered_exchanges, covered));
    providers
}

