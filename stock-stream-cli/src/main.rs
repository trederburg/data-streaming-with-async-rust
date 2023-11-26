use chrono::prelude::*;
use clap::Parser;

mod file_actor;
mod request_actor;
mod stock_signal;


#[derive(Parser, Debug)]
#[clap(
    version = "1.1",
    author = "trederburg",
    about = "Async Stock Single Fetcher Using Actors"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

#[tokio::main]
async fn main() -> () {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    
}
