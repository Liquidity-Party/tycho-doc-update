//! Hybrid Curve implementation: pure-Rust quote math via the vendored `math` / `adapter` modules,
//! with pool state read from the locally indexed VM storage at decode and update time.
//!
//! Indexing (the `vm:curve` substreams) and the on-chain executor are unchanged; only the consumer
//! decoder differs — register [`CurveState`] for `"vm:curve"` instead of the generic VM adapter.
//!
//! `math` and `adapter` are vendored from the MIT-licensed `curve-math` / `curve-adapter` crates
//! (see `LICENSE-curve-math`); the post-MIT features (StableSwapSTETH, TwoCryptoV1 `eth_variant`,
//! the ALend `+1` `get_D` quirk) are re-derived from Curve's public Vyper sources.
mod adapter;
mod decoder;
mod math;
mod state;
mod variant;
mod vm;

pub use state::CurveState;
