use barter_data::{
    exchange::{
        binance::{futures::BinanceFuturesUsd, spot::BinanceSpot}, coinbase::Coinbase, gateio::spot::GateioSpot, kraken::Kraken, okx::Okx
    },
    streams::{reconnect::stream::ReconnectingStream, Streams},
    subscription::trade::PublicTrades,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use futures::StreamExt;
use tracing::warn;

#[tokio::main]
async fn main() {
    // Initialise PublicTrades Streams for various exchanges
    // '--> each call to StreamBuilder::subscribe() initialises a separate WebSocket connection

    let streams = Streams::<PublicTrades>::builder()
            .subscribe([
            (Kraken::default(), "MATIC", "USD", MarketDataInstrumentKind::Spot, PublicTrades),
            // (BinanceSpot::default(), "eth", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
        ])
        // .subscribe([
        //     (BinanceSpot::default(), "btc", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
        //     (BinanceSpot::default(), "eth", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
        // ])
    //     .subscribe([
    //         (BinanceFuturesUsd::default(), "btc", "usdt", MarketDataInstrumentKind::Perpetual, PublicTrades),
    //         (BinanceFuturesUsd::default(), "eth", "usdt", MarketDataInstrumentKind::Perpetual, PublicTrades),
    //     ])
    //     .subscribe([
    //         (Coinbase, "btc", "usd", MarketDataInstrumentKind::Spot, PublicTrades),
    //         (Coinbase, "eth", "usd", MarketDataInstrumentKind::Spot, PublicTrades),
    //     ])
    //     .subscribe([
    //         (GateioSpot::default(), "btc", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
    //         (GateioSpot::default(), "eth", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
    //     ])
    //     .subscribe([
    //         (Okx, "btc", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
    //         (Okx, "eth", "usdt", MarketDataInstrumentKind::Spot, PublicTrades),
    //         (Okx, "btc", "usdt", MarketDataInstrumentKind::Perpetual, PublicTrades),
    //         (Okx, "eth", "usdt", MarketDataInstrumentKind::Perpetual, PublicTrades),
    //    ]
    // )
        .init()
        .await
        .unwrap();

    // Select and merge every exchange Stream using futures_util::stream::select_all
    // Note: use `Streams.select(ExchangeId)` to interact with individual exchange streams!
    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        println!("{event:?}");
    }
}