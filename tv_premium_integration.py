#!/usr/bin/env python3
"""
Custom TradingView Intra-Day Screen Downloader
Downloads your specific screener every 14 minutes
Uses your exact filters from TradingView
"""

import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import logging
import schedule
from typing import Dict, List, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntraDayScreenerDownloader:
    """
    Downloads your Intra-Day Screen every 14 minutes
    1 minute before each 15-minute candle closes
    """

    def __init__(self, output_dir: str = "./intraday_screener_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Market hours (PT)
        self.market_tz = pytz.timezone('US/Pacific')
        self.market_open = (6, 30)  # 6:30 AM
        self.market_close = (13, 0)  # 1:00 PM

        # TradingView Scanner API endpoint
        self.scanner_url = "https://scanner.tradingview.com/america/scan"

        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Origin': 'https://www.tradingview.com',
            'Referer': 'https://www.tradingview.com/'
        })

        # Download tracking
        self.download_count = 0
        self.last_download = None
        self.last_data = None

    def get_intraday_screener_payload(self) -> Dict:
        """
        Your exact Intra-Day Screen configuration from TradingView
        """
        return {
            "columns": [
                # Identification
                "name", "description", "logoid", "exchange",

                # Price and Volume
                "close", "open", "high", "low", "volume",
                "change", "change_abs", "gap",
                "average_volume_10d_calc", "relative_volume_10d_calc",
                "VWAP", "VWMA",

                # Technical Ratings
                "TechRating_1D", "TechRating_1D.tr",
                "MARating_1D", "MARating_1D.tr",
                "OsRating_1D", "OsRating_1D.tr",

                # Momentum Indicators (your selection)
                "RSI", "Mom", "AO", "CCI20",
                "Stoch.K", "Stoch.D",

                # Additional Technical
                "MACD.macd", "MACD.signal",
                "ATR", "ADR", "Volatility.D",
                "BB.upper", "BB.lower",

                # Moving Averages for 15-min timeframe
                "EMA20|15", "EMA40|15", "SMA20|15",

                # Candle Patterns (your selection)
                "Candle.3BlackCrows", "Candle.3WhiteSoldiers",
                "Candle.AbandonedBaby.Bearish", "Candle.AbandonedBaby.Bullish",
                "Candle.Doji", "Candle.Doji.Dragonfly", "Candle.Doji.Gravestone",
                "Candle.Engulfing.Bearish", "Candle.Engulfing.Bullish",
                "Candle.EveningStar", "Candle.MorningStar",
                "Candle.Hammer", "Candle.HangingMan",
                "Candle.Harami.Bearish", "Candle.Harami.Bullish",
                "Candle.InvertedHammer", "Candle.ShootingStar",
                "Candle.SpinningTop.Black", "Candle.SpinningTop.White",

                # Other
                "beta_1_year", "pricescale", "minmov"
            ],

            "filter": [
                # Your exact filters
                {"left": "close", "operation": "in_range", "right": [8, 100]},
                {"left": "change", "operation": "greater", "right": 0},
                {"left": "ADR", "operation": "egreater", "right": 1},
                {"left": "EMA40|15", "operation": "less", "right": "close|15"},
                {"left": "average_volume_10d_calc", "operation": "greater", "right": 2000000}
            ],

            "filter2": {
                "operator": "and",
                "operands": [
                    {
                        "operation": {
                            "operator": "or",
                            "operands": [
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                            {"expression": {"left": "typespecs", "operation": "has",
                                                            "right": ["common"]}}
                                        ]
                                    }
                                },
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                            {"expression": {"left": "typespecs", "operation": "has",
                                                            "right": ["preferred"]}}
                                        ]
                                    }
                                },
                                {"operation": {"operator": "and", "operands": [
                                    {"expression": {"left": "type", "operation": "equal", "right": "dr"}}]}},
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "fund"}},
                                            {"expression": {"left": "typespecs", "operation": "has_none_of",
                                                            "right": ["etf"]}}
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    {"expression": {"left": "typespecs", "operation": "has_none_of", "right": ["pre-ipo"]}}
                ]
            },

            "ignore_unknown_fields": False,
            "options": {"lang": "en"},
            "range": [0, 500],  # Get up to 500 stocks
            "sort": {"sortBy": "relative_volume_10d_calc", "sortOrder": "desc"},
            # Changed to desc for highest volume first
            "markets": ["america"]
        }

    def download_screener(self) -> pd.DataFrame:
        """
        Download your Intra-Day Screen data

        Returns:
            DataFrame with screener results
        """
        try:
            payload = self.get_intraday_screener_payload()

            logger.info("📥 Downloading Intra-Day Screen...")

            response = self.session.post(
                self.scanner_url,
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            # Parse response
            if "data" in data and data["data"]:
                df = self.parse_response(data, payload["columns"])

                # Add metadata
                df['download_time'] = datetime.now().isoformat()
                df['candle_15min'] = self.get_next_candle_time()

                # Calculate additional fields
                if 'close' in df.columns and 'VWAP' in df.columns:
                    df['above_vwap'] = df['close'] > df['VWAP']
                    df['vwap_distance'] = ((df['close'] - df['VWAP']) / df['VWAP'] * 100).round(2)

                logger.info(f"✅ Downloaded {len(df)} stocks matching your filters")

                # Log summary
                self.log_summary(df)

                return df
            else:
                logger.warning("No data in response")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Download failed: {e}")
            return pd.DataFrame()

    def parse_response(self, data: Dict, columns: List[str]) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        rows = []

        for item in data.get("data", []):
            row = {"ticker": item.get("s", "")}

            values = item.get("d", [])
            for i, col in enumerate(columns):
                if i < len(values):
                    row[col] = values[i]

            rows.append(row)

        return pd.DataFrame(rows)

    def log_summary(self, df: pd.DataFrame):
        """Log summary statistics"""
        if df.empty:
            return

        try:
            avg_volume_ratio = df['relative_volume_10d_calc'].mean() if 'relative_volume_10d_calc' in df else 0
            avg_change = df['change'].mean() if 'change' in df else 0

            logger.info(f"📊 Summary:")
            logger.info(f"   Total stocks: {len(df)}")
            logger.info(f"   Avg volume ratio: {avg_volume_ratio:.2f}x")
            logger.info(f"   Avg change: {avg_change:.2f}%")

            # Top movers
            if 'change' in df.columns:
                top_gainers = df.nlargest(3, 'change')
                logger.info(f"   Top gainers: {', '.join(top_gainers['name'].tolist())}")

            # Highest volume
            if 'relative_volume_10d_calc' in df.columns:
                high_volume = df.nlargest(3, 'relative_volume_10d_calc')
                logger.info(f"   High volume: {', '.join(high_volume['name'].tolist())}")

        except Exception as e:
            logger.debug(f"Summary failed: {e}")

    def save_data(self, df: pd.DataFrame) -> str:
        """Save downloaded data"""
        if df.empty:
            logger.warning("No data to save")
            return None

        timestamp = datetime.now()

        # Main CSV file with timestamp
        filename = f"intraday_{timestamp.strftime('%Y%m%d_%H%M')}.csv"
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=False)

        # Latest file for easy access
        df.to_csv(self.output_dir / "latest.csv", index=False)

        # Symbols only for scanner
        symbols = df['name'].tolist() if 'name' in df else []
        if symbols:
            symbols_file = self.output_dir / "symbols_latest.txt"
            with open(symbols_file, 'w') as f:
                for symbol in symbols:
                    f.write(f"{symbol}\n")

            # Top 20 high priority symbols
            priority_file = self.output_dir / "priority_symbols.txt"
            with open(priority_file, 'w') as f:
                for symbol in symbols[:20]:
                    f.write(f"{symbol}\n")

        # Scanner input JSON
        scanner_input = {
            "timestamp": timestamp.isoformat(),
            "candle": self.get_next_candle_time(),
            "screen": "Intra-Day Screen",
            "total_stocks": len(df),
            "filters": {
                "price_range": "$8-$100",
                "min_volume": "2M avg",
                "change": "positive",
                "ADR": ">1",
                "EMA40_15min": "price above"
            },
            "top_symbols": symbols[:20] if symbols else [],
            "data": df.head(50).to_dict('records')
        }

        scanner_file = self.output_dir / "scanner_input.json"
        with open(scanner_file, 'w') as f:
            json.dump(scanner_input, f, indent=2, default=str)

        self.download_count += 1
        self.last_download = timestamp
        self.last_data = df

        logger.info(f"💾 Saved to {filepath}")
        logger.info(f"📝 {len(symbols)} symbols ready for scanner")

        return str(filepath)

    def get_next_candle_time(self) -> str:
        """Get the next 15-minute candle close time"""
        now = datetime.now()
        minute = now.minute

        # Next 15-min mark
        next_candle = ((minute // 15) + 1) * 15

        if next_candle >= 60:
            hour = (now.hour + 1) % 24
            next_candle = 0
        else:
            hour = now.hour

        return f"{hour:02d}:{next_candle:02d}"

    def is_market_hours(self) -> bool:
        """Check if market is open"""
        now = datetime.now(self.market_tz)

        # Skip weekends
        if now.weekday() > 4:
            return False

        # Check time
        current_time = (now.hour, now.minute)
        return self.market_open <= current_time < self.market_close

    def download_job(self):
        """Job to run every 14 minutes"""
        if not self.is_market_hours():
            logger.info("Market closed, skipping download")
            return

        logger.info(f"\n{'=' * 50}")
        logger.info(f"⏰ Scheduled download at {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"   Next candle: {self.get_next_candle_time()}")

        df = self.download_screener()

        if not df.empty:
            filepath = self.save_data(df)
            logger.info(f"✅ Download #{self.download_count} complete")
        else:
            logger.warning("⚠️ No data downloaded")

    def test_download(self):
        """Test single download"""
        logger.info("\n🧪 Testing Intra-Day Screen download...")

        df = self.download_screener()

        if not df.empty:
            print(f"\n✅ SUCCESS! Downloaded {len(df)} stocks")

            # Show sample data
            print("\n📊 Sample data (top 5):")
            display_cols = ['name', 'close', 'change', 'volume', 'relative_volume_10d_calc', 'RSI']
            available_cols = [col for col in display_cols if col in df.columns]

            print(df[available_cols].head(5).to_string(index=False))

            # Save test file
            test_file = "test_intraday_screen.csv"
            df.to_csv(test_file, index=False)
            print(f"\n💾 Test data saved to: {test_file}")

            return True
        else:
            print("\n❌ Download failed or no stocks match filters")
            return False

    def run_scheduled(self):
        """Run on schedule - every 15 minutes at :14, :29, :44, :59"""

        print("\n" + "=" * 60)
        print("📊 INTRA-DAY SCREENER - SCHEDULED DOWNLOADER")
        print("=" * 60)
        print(f"Screen: Intra-Day Screen")
        print(f"Filters: Price $8-100 | Change > 0 | ADR > 1 | Vol > 2M")
        print(f"Schedule: Every 14 minutes (before 15-min candles)")
        print(f"Output: {self.output_dir}/")
        print("=" * 60)

        # Schedule downloads
        schedule.every().hour.at(":14").do(self.download_job)
        schedule.every().hour.at(":29").do(self.download_job)
        schedule.every().hour.at(":44").do(self.download_job)
        schedule.every().hour.at(":59").do(self.download_job)

        # Run once at start if market is open
        if self.is_market_hours():
            logger.info("\nMarket is open, running initial download...")
            self.download_job()
        else:
            logger.info("\nMarket is closed, waiting for next scheduled time...")

        logger.info("\n⏳ Scheduler running. Press Ctrl+C to stop.\n")

        # Main loop
        while True:
            try:
                schedule.run_pending()

                # Status update every 5 minutes
                if datetime.now().minute % 5 == 0 and datetime.now().second < 1:
                    if self.last_download:
                        elapsed = (datetime.now() - self.last_download).seconds // 60
                        logger.info(f"📍 Status: Running | Downloads: {self.download_count} | Last: {elapsed}m ago")

                time.sleep(1)

            except KeyboardInterrupt:
                logger.info("\n👋 Stopping scheduler...")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                time.sleep(60)


def main():
    """Main entry point"""
    print("\n" + "🚀" * 20)
    print(" INTRA-DAY SCREEN DOWNLOADER ")
    print("🚀" * 20)

    print("\nYour custom screen configuration:")
    print("  • Price: $8 - $100")
    print("  • Change: Positive (>0%)")
    print("  • ADR: >1")
    print("  • EMA40 (15min): Price above")
    print("  • Volume: >2M average")

    print("\n1. Test download (verify it works)")
    print("2. Run scheduled (every 14 minutes)")
    print("3. Download once now")

    choice = input("\nSelect (1-3): ")

    downloader = IntraDayScreenerDownloader()

    if choice == "1":
        # Test mode
        success = downloader.test_download()
        if success:
            print("\n✅ Test successful! Ready to run scheduled.")
            if input("\nStart scheduler now? (y/n): ").lower() == 'y':
                downloader.run_scheduled()

    elif choice == "2":
        # Scheduled mode
        downloader.run_scheduled()

    elif choice == "3":
        # Single download
        df = downloader.download_screener()
        if not df.empty:
            filepath = downloader.save_data(df)
            print(f"\n✅ Downloaded {len(df)} stocks")
            print(f"💾 Saved to: {filepath}")
        else:
            print("\n❌ No data downloaded")

    else:
        print("Invalid choice")


if __name__ == "__main__":
    import sys

    downloader = IntraDayScreenerDownloader()
    downloader.test_download()
    # if len(sys.argv) > 1:
    #     if sys.argv[1] == "--test":
    #         # Quick test: python script.py --test
    #         downloader = IntraDayScreenerDownloader()
    #         downloader.test_download()
    #     elif sys.argv[1] == "--run":
    #         # Direct run: python script.py --run
    #         downloader = IntraDayScreenerDownloader()
    #         downloader.run_scheduled()
    # else:
    #     main()