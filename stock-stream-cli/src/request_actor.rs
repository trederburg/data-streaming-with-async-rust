use chrono::{DateTime, Utc};
use yahoo::time::OffsetDateTime;
use yahoo_finance_api as yahoo;
use std::io::{Error, ErrorKind};

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
///
/// Helper function for converting chrono Dateime into Offset Datetime
/// 
fn convert_datetime_to_offset(datetime: &DateTime<Utc>) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(datetime.timestamp()).expect("Couldn't convert DateTime from chrono to Offset")
}
