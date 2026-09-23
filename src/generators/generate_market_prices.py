import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


# Open and read the project configuration
with open("config/data_generation.yml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


# Read the required development record counts
price_count = config["datasets"]["market_prices"][
    "development_records"
]

security_count = config["datasets"]["securities"][
    "development_records"
]


# Produce the same random results each time
random.seed(46)


# Store the current time
current_time = datetime.now(timezone.utc)


# Empty list for storing market prices
market_prices = []


# Generate market-price records
for number in range(1, price_count + 1):

    security_number = (
        (number - 1) % security_count
    ) + 1

    base_price = random.uniform(10.00, 500.00)

    open_price = base_price * random.uniform(0.98, 1.02)
    close_price = base_price * random.uniform(0.98, 1.02)

    high_price = max(
        open_price,
        close_price,
    ) * random.uniform(1.00, 1.03)

    low_price = min(
        open_price,
        close_price,
    ) * random.uniform(0.97, 1.00)

    price_timestamp = current_time - timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    market_price = {
        "price_id": f"PRC{number:010d}",
        "security_id": f"SEC{security_number:06d}",
        "open_price": round(open_price, 2),
        "high_price": round(high_price, 2),
        "low_price": round(low_price, 2),
        "close_price": round(close_price, 2),
        "trading_volume": random.randint(
    1_000,
    1_000_000,
),
        "price_timestamp": price_timestamp.isoformat(),
        "source_system": "MARKET_DATA_FEED",
        "ingestion_timestamp": current_time.isoformat(),
    }

    market_prices.append(market_price)


# Create the output folder
output_folder = Path("data/generated/market_prices")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the records into a table
market_prices_table = pa.Table.from_pylist(
    market_prices
)


# Save the table as a Parquet file
output_file = output_folder / "market_prices.parquet"

pq.write_table(
    market_prices_table,
    output_file,
    compression="snappy",
)


print(f"Generated market prices: {len(market_prices):,}")
print(f"Saved to: {output_file}")
