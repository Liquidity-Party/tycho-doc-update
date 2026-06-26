//! Vendored Curve AMM math (`math`) and pool-construction adapter (`adapter`), inlined from the
//! MIT-licensed `curve-math` / `curve-adapter` crates (see `LICENSE-curve-math`).
//!
//! These provide the pure-Rust quote math. The hybrid `vm:curve` decoder that wires them into
//! tycho-simulation (reading pool state from the locally indexed VM) is added on top of this.
mod adapter;
mod math;
