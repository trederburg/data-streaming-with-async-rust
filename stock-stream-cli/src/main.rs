use actix::prelude::*;
use chrono::prelude::*;
use clap::Parser;
use std::io::{Error, ErrorKind};
use tokio::{self};
use yahoo::time::OffsetDateTime;
use yahoo_finance_api as yahoo;
mod stock_signal;
use crate::stock_signal::{AsyncStockSignal, MaxPrice, MinPrice, PriceDifference, WindowedSMA};
use actix_rt;

#[derive(Parser, Debug)]
#[clap(
    version = "1.1",
    author = "Claus Matzinger, trederburg",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}
struct DataFetcher {
    data_processor: Addr<DataProcessor>,
}

impl Actor for DataFetcher {
    type Context = Context<Self>;
}

struct FetchData {
    symbol: String,
    from: OffsetDateTime,
    to: OffsetDateTime,
}

impl Message for FetchData {
    type Result = Result<(), Box<dyn std::error::Error + 'static + Send>>;
}

//#[async_trait]
impl Handler<FetchData> for DataFetcher {
    type Result = ResponseFuture<Result<(), Box<dyn std::error::Error + 'static + Send>>>;

    fn handle(&mut self, msg: FetchData, _ctx: &mut Context<Self>) -> Self::Result {
        let symbol = msg.symbol.clone();
        let from = msg.from.clone();
        let to = msg.to.clone();
        let data_processor_addr = self.data_processor.clone();
        Box::pin(async move {
            let result = fetch_closing_data(&symbol, from, to).await;
            match result {
                Ok(closes) => {
                    let _ = data_processor_addr
                        .send(Convert {
                            symbol,
                            closes,
                            to,
                            from,
                        })
                        .await;
                    Ok(())
                }
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + 'static + Send>),
            }
        })
    }
}
struct DataProcessor;

impl Actor for DataProcessor {
    type Context = Context<Self>;
}
struct Convert {
    symbol: String,
    closes: Vec<f64>,
    to: OffsetDateTime,
    from: OffsetDateTime,
}

impl Message for Convert {
    type Result = String;
}

impl Handler<Convert> for DataProcessor {
    type Result = String;

    fn handle(&mut self, msg: Convert, _ctx: &mut Context<Self>) -> Self::Result {
        convert_closes_to_string(msg.from, msg.to, msg.symbol.as_str(), msg.closes)
    }
}
// const BUFFER_SIZE: usize = 100;
// const REFRESH_INTERVAL: u64 = 30;
const WINDOW_SIZE: usize = 30;
///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: OffsetDateTime,
    end: OffsetDateTime,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(symbol, beginning, end)
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

// async fn generate_yahoo_requests(
//     from: &DateTime<Utc>,
//     symbol: &str,
// ) -> Vec<f64> {
//     let from = from.clone();
//     let symbol = symbol.to_string();
//     tokio::spawn(async move {
//         // let mut interval = interval(Duration::from_secs(REFRESH_INTERVAL));
//         // loop {
//             let symbol = symbol.to_string();
//             let to = Utc::now();
//             let closes = fetch_closing_data(&symbol, &from, &to)
//                 .await
//                 .unwrap_or_else(|_| vec![]);
//             (symbol, closes, to)
//             // interval.tick().await;
//         // }
//     });
// }

fn convert_closes_to_string(
    from: OffsetDateTime,
    to: OffsetDateTime,
    symbol: &str,
    closes: Vec<f64>,
) -> String {
    // min/max of the period. unwrap() because those are Option types
    let period_max = MaxPrice.calculate(&closes).unwrap();
    let period_min = MinPrice.calculate(&closes).unwrap();
    let last_price = *closes.last().unwrap_or(&0.0);
    let (_, pct_change) = PriceDifference.calculate(&closes).unwrap_or((0.0, 0.0));
    let sma = WindowedSMA::new(WINDOW_SIZE)
        .calculate(&closes)
        .unwrap_or_default();

    // a simple way to output CSV data
    format!(
        "{},{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
        from.unix_timestamp(),
        to.unix_timestamp(),
        symbol,
        last_price,
        pct_change * 100.0,
        period_min,
        period_max,
        sma.last().unwrap_or(&0.0)
    )
}

// #[tokio::main]
// async fn main() -> () {
//     let opts = Opts::parse();
//     let from: OffsetDateTime = OffsetDateTime::checked_to_offset(self, offset)// opts.from.

//     // a simple way to output a CSV header
//     println!("period start,period end,symbol,price,change %,min,max,30d avg");
//     let (tx, mut rx) = mpsc::channel(BUFFER_SIZE);
//     for symbol in opts.symbols.split(',') {
//         generate_yahoo_requests(tx.clone(), &from, symbol).await;
//     }
//     while let Some((symbol, closes, to)) = rx.recv().await {
//         if !closes.is_empty() {
//             println!(
//                 "{}",
//                 convert_closes_to_string(from, to, symbol.as_str(), closes)
//             );
//         }
//     }
// }

#[actix::main]
async fn main() {
    let opts = Opts::parse();
    let from: DateTime<Utc> = match opts.from.parse() {
        Ok(from) => from,
        Err(_) => {
            eprint!("Could not parse 'from'");
            Utc::now()
        }
    };
    let to: DateTime<Utc> = Utc::now();
    println!("period start,period end,symbol,price,change %,min,max,30d avg");

    let data_processor = DataProcessor.start();

    let symbols: Vec<&str> = opts.symbols.split(',').collect();
    let data_fetcher = DataFetcher { data_processor }.start();
    symbols.into_iter().for_each(|symbol| {
        let symbol = symbol.to_string();
        let data_fetcher = data_fetcher.clone();
        actix_rt::spawn(async move {
            let result = data_fetcher
                .send(FetchData {
                    symbol: symbol.to_owned(),
                    from: OffsetDateTime::from_unix_timestamp(from.timestamp()).unwrap(),
                    to: OffsetDateTime::from_unix_timestamp(to.timestamp()).unwrap(),
                })
                .await;
            match result {
                Ok(_) => {
                    println!("Success")
                }
                Err(e) => {
                    eprintln!("{:?}", e)
                }
            }
        });
    });
}
