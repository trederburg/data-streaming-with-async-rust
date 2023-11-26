use chrono::{DateTime, Utc};
use crate::stock_signal::{AsyncStockSignal, MaxPrice, MinPrice, PriceDifference, WindowedSMA};

const WINDOW_SIZE: usize = 30;
const HEADER: &str = "period start,period end,symbol,price,change %,min,max,30d avg";

///
/// converts 'to' 'from' datetimes and 'closes' Vec<f64> into String of 'period start,period end,symbol,price,change %,min,max, 30d avg' 
/// 
fn convert_closes_to_string(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
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
        from.to_rfc3339(),
        to.to_rfc3339(),
        symbol,
        last_price,
        pct_change * 100.0,
        period_min,
        period_max,
        sma.last().unwrap_or(&0.0)
    )
}