/*
 * Kalshi Extension Methods
 * Helpers for Kalshi data conversion
 */

using NodaTime;
using QuantConnect.Data.Market;
using QuantConnect.Lean.DataSource.KalshiData.Models;

namespace QuantConnect.Lean.DataSource.KalshiData
{
    /// <summary>
    /// Extension methods for Kalshi data conversion
    /// </summary>
    public static class KalshiExtensions
    {
        /// <summary>
        /// Eastern Time zone for Kalshi markets
        /// </summary>
        public static readonly DateTimeZone KalshiTimeZone = TimeZones.NewYork;

        /// <summary>
        /// Convert cents (0-100) to decimal probability (0.00-1.00)
        /// </summary>
        public static decimal CentsToDecimal(this int cents)
        {
            return cents / 100m;
        }

        /// <summary>
        /// Convert nullable cents (0-100) to decimal probability (0.00-1.00)
        /// </summary>
        public static decimal CentsToDecimal(this int? cents)
        {
            return (cents ?? 0) / 100m;
        }

        /// <summary>
        /// Convert Unix timestamp to DateTime
        /// </summary>
        public static DateTime UnixSecondsToDateTime(this long unixSeconds)
        {
            return DateTimeOffset.FromUnixTimeSeconds(unixSeconds).DateTime;
        }

        /// <summary>
        /// Convert Unix timestamp to DateTime in a specific timezone
        /// </summary>
        public static DateTime UnixSecondsToDateTime(this long unixSeconds, DateTimeZone targetZone)
        {
            var utcDateTime = DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
            return utcDateTime.ConvertFromUtc(targetZone);
        }

        /// <summary>
        /// Convert DateTime to Unix timestamp in seconds
        /// </summary>
        public static long ToUnixSeconds(this DateTime dateTime)
        {
            return new DateTimeOffset(dateTime, TimeSpan.Zero).ToUnixTimeSeconds();
        }

        /// <summary>
        /// Convert DateTime to Unix timestamp, assuming it's in the specified timezone
        /// </summary>
        public static long ToUnixSeconds(this DateTime dateTime, DateTimeZone sourceZone)
        {
            var utcDateTime = dateTime.ConvertToUtc(sourceZone);
            return new DateTimeOffset(utcDateTime, TimeSpan.Zero).ToUnixTimeSeconds();
        }

        /// <summary>
        /// Convert a Kalshi candlestick to a LEAN QuoteBar.
        /// Candlesticks endpoint provides consistent minute-level data.
        /// </summary>
        public static QuoteBar ToQuoteBar(this KalshiCandlestick candle, Symbol symbol, TimeSpan period, DateTimeZone exchangeTimeZone)
        {
            var endTime = candle.EndPeriodTs.UnixSecondsToDateTime(exchangeTimeZone);
            var startTime = endTime - period;

            var quoteBar = new QuoteBar
            {
                Symbol = symbol,
                Time = startTime,
                Period = period
            };

            // Convert bid OHLC (cents to decimal)
            if (candle.YesBid != null)
            {
                quoteBar.Bid = new Bar(
                    candle.YesBid.Open.CentsToDecimal(),
                    candle.YesBid.High.CentsToDecimal(),
                    candle.YesBid.Low.CentsToDecimal(),
                    candle.YesBid.Close.CentsToDecimal()
                );
                quoteBar.LastBidSize = candle.Volume;
            }

            // Convert ask OHLC (cents to decimal)
            if (candle.YesAsk != null)
            {
                quoteBar.Ask = new Bar(
                    candle.YesAsk.Open.CentsToDecimal(),
                    candle.YesAsk.High.CentsToDecimal(),
                    candle.YesAsk.Low.CentsToDecimal(),
                    candle.YesAsk.Close.CentsToDecimal()
                );
                quoteBar.LastAskSize = candle.Volume;
            }

            return quoteBar;
        }

        /// <summary>
        /// Convert a Kalshi candlestick to a NO token QuoteBar by inverting YES prices.
        /// NO_BID = 1 - YES_ASK, NO_ASK = 1 - YES_BID.
        /// OHLC inversion: High/Low swap because 1-x is monotone decreasing.
        /// </summary>
        public static QuoteBar ToNoQuoteBar(this KalshiCandlestick candle, Symbol noSymbol, TimeSpan period, DateTimeZone exchangeTimeZone)
        {
            var endTime = candle.EndPeriodTs.UnixSecondsToDateTime(exchangeTimeZone);
            var startTime = endTime - period;

            var quoteBar = new QuoteBar
            {
                Symbol = noSymbol,
                Time = startTime,
                Period = period
            };

            // NO_BID = 1 - YES_ASK (what you receive selling NO = complement of YES ask)
            // OHLC inversion: YES_ASK High → NO Bid Low, YES_ASK Low → NO Bid High
            if (candle.YesAsk != null)
            {
                quoteBar.Bid = new Bar(
                    (100 - candle.YesAsk.Open).CentsToDecimal(),
                    (100 - candle.YesAsk.Low).CentsToDecimal(),     // YES low → NO bid high
                    (100 - candle.YesAsk.High).CentsToDecimal(),    // YES high → NO bid low
                    (100 - candle.YesAsk.Close).CentsToDecimal()
                );
                quoteBar.LastBidSize = candle.Volume;
            }

            // NO_ASK = 1 - YES_BID (what you pay buying NO = complement of YES bid)
            // OHLC inversion: YES_BID High → NO Ask Low, YES_BID Low → NO Ask High
            if (candle.YesBid != null)
            {
                quoteBar.Ask = new Bar(
                    (100 - candle.YesBid.Open).CentsToDecimal(),
                    (100 - candle.YesBid.Low).CentsToDecimal(),     // YES low → NO ask high
                    (100 - candle.YesBid.High).CentsToDecimal(),    // YES high → NO ask low
                    (100 - candle.YesBid.Close).CentsToDecimal()
                );
                quoteBar.LastAskSize = candle.Volume;
            }

            return quoteBar;
        }

        /// <summary>
        /// Convert a Kalshi candlestick to a LEAN TradeBar (using trade price if available)
        /// </summary>
        public static TradeBar? ToTradeBar(this KalshiCandlestick candle, Symbol symbol, TimeSpan period, DateTimeZone exchangeTimeZone)
        {
            if (candle.Price?.IsValid != true)
            {
                return null;
            }

            var endTime = candle.EndPeriodTs.UnixSecondsToDateTime(exchangeTimeZone);
            var startTime = endTime - period;

            return new TradeBar(
                startTime,
                symbol,
                candle.Price.Open.CentsToDecimal(),
                candle.Price.High.CentsToDecimal(),
                candle.Price.Low.CentsToDecimal(),
                candle.Price.Close.CentsToDecimal(),
                candle.Volume,
                period
            );
        }

        /// <summary>
        /// Generate date ranges for chunked API requests
        /// </summary>
        public static IEnumerable<(DateTime startDate, DateTime endDate)> GenerateDateRanges(
            DateTime startDate,
            DateTime endDate,
            int intervalDays = 3)
        {
            var current = startDate;
            while (current < endDate)
            {
                var rangeEnd = current.AddDays(intervalDays);
                if (rangeEnd > endDate)
                {
                    rangeEnd = endDate;
                }
                yield return (current, rangeEnd);
                current = rangeEnd;
            }
        }
    }
}
