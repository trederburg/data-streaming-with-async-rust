use chrono::prelude::*;
use clap::Parser;
use std::io::{Error, ErrorKind};
use tokio::{self, sync::mpsc};
use yahoo::time::OffsetDateTime;
use yahoo_finance_api as yahoo;
mod stock_signal;
use crate::stock_signal::{AsyncStockSignal, MaxPrice, MinPrice, PriceDifference, WindowedSMA};

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(
            symbol,
            convert_datetime_to_offset(beginning),
            convert_datetime_to_offset(end),
        )
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

fn convert_datetime_to_offset(datetime: &DateTime<Utc>) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(datetime.timestamp()).expect("Couldn't convert")
}

fn convert_closes_to_string(from: DateTime<Utc>, symbol: &str, closes: Vec<f64>) -> String {
    // min/max of the period. unwrap() because those are Option types
    let period_max = MaxPrice.calculate(&closes).unwrap();
    let period_min = MinPrice.calculate(&closes).unwrap();
    let last_price = *closes.last().unwrap_or(&0.0);
    let (_, pct_change) = PriceDifference.calculate(&closes).unwrap_or((0.0, 0.0));
    let sma = WindowedSMA { window_size: 30 }
        .calculate(&closes)
        .unwrap_or_default();

    // a simple way to output CSV data
    format!(
        "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
        from.to_rfc3339(),
        symbol,
        last_price,
        pct_change * 100.0,
        period_min,
        period_max,
        sma.last().unwrap_or(&0.0)
    )
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let to: DateTime<Utc> = Utc::now();

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    let (tx, mut rx) = mpsc::channel(100);
    for symbol in opts.symbols.split(',') {
        let tx = tx.clone();
        let from = from.clone();
        let to = to.clone();
        let symbol = symbol.to_string();
        tokio::spawn(async move {
            let closes = fetch_closing_data(&symbol, &from, &to)
                .await
                .unwrap_or_else(|_| vec![]);
            let _ = tx.send((symbol, closes)).await;
        });
    }
    drop(tx);

    while let Some((symbol, closes)) = rx.recv().await {
        if !closes.is_empty() {
            println!(
                "{}",
                convert_closes_to_string(from, symbol.as_str(), closes)
            );
        }
    }
    Ok(())
}
