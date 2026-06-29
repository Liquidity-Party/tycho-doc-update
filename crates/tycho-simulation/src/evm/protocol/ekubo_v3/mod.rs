use revm::primitives::Address;
use tycho_client::feed::synchronizer::ComponentWithState;

use crate::evm::protocol::ekubo_v3::addresses::{
    BOOSTED_FEES_CONCENTRATED_ADDRESS, MEV_CAPTURE_ADDRESS, ORACLE_ADDRESS, TWAMM_ADDRESS,
};

mod addresses;
mod attributes;
mod decoder;
mod pool;
pub mod state;

#[cfg(test)]
mod test_cases;

pub enum ExtensionType {
    NoSwapCallPoints,
    Oracle,
    Twamm,
    MevCapture,
    BoostedFees,
}

fn has_no_swap_call_points(extension: Address) -> bool {
    // Call points are encoded in the first byte of the extension address.
    // Bit 6 == beforeSwap, bit 5 == afterSwap.
    extension[0] & 0b0110_0000 == 0
}

pub fn extension_type(extension: Address) -> Option<ExtensionType> {
    Some(if has_no_swap_call_points(extension) {
        ExtensionType::NoSwapCallPoints
    } else if extension == ORACLE_ADDRESS {
        ExtensionType::Oracle
    } else if extension == TWAMM_ADDRESS {
        ExtensionType::Twamm
    } else if extension == MEV_CAPTURE_ADDRESS {
        ExtensionType::MevCapture
    } else if extension == BOOSTED_FEES_CONCENTRATED_ADDRESS {
        ExtensionType::BoostedFees
    } else {
        return None;
    })
}

/// Filters out unsupported ekubo_v3 extensions.
#[deprecated(
    note = "Use `tycho_simulation::evm::protocol::filters::ekubo_v3_extension_filter` instead."
)]
pub fn filter_fn(component: &ComponentWithState) -> bool {
    component
        .component
        .static_attributes
        .get("extension")
        .is_some_and(|extension_bytes| {
            Address::try_from(&extension_bytes[..])
                .is_ok_and(|extension| extension_type(extension).is_some())
        })
}
