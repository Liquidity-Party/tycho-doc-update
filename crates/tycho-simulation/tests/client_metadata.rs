//! Verifies `ProtocolStreamBuilder` forwards client metadata to the wrapped `TychoStreamBuilder`.

use std::time::Duration;

use tycho_simulation::{
    evm::{protocol::uniswap_v2::state::UniswapV2State, stream::ProtocolStreamBuilder},
    tycho_client::{feed::component_tracker::ComponentFilter, stream::StreamError},
    tycho_common::models::Chain,
};

#[tokio::test]
async fn client_metadata_passthrough_fails_fast_on_invalid() {
    let build = ProtocolStreamBuilder::new("localhost:4242", Chain::Ethereum)
        .exchange::<UniswapV2State>(
            "uniswap_v2",
            ComponentFilter::with_tvl_range(100.0, 100.0),
            None,
        )
        .client_metadata_entry("bad key", "v")
        .build();
    // The invalid entry is only rejected if it was forwarded to the inner builder, and the
    // rejection happens at build() before any network I/O — so a short timeout is enough.
    let result = tokio::time::timeout(Duration::from_secs(2), build)
        .await
        .expect("build should fail fast without network I/O");
    assert!(matches!(result, Err(StreamError::SetUpError(_))));
}
