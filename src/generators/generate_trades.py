import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


# Read the project configuration
with open("config/data_generation.yml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


# Read the development record counts
trade_count = config["datasets"]["trades"][
    "development_records"
]

account_count = config["datasets"]["accounts"][
    "development_records"
]

security_count = config["datasets"]["securities"][
    "development_records"
]

counterparty_count = config["datasets"]["counterparties"][
    "development_records"
]


# Use the same random results whenever the program runs
random.seed(45)


# Store the current time
current_time = datetime.now(timezone.utc)


# Empty list for storing trades
trades = []


# Generate the trade records
for number in range(1, trade_count + 1):

    trade_timestamp = current_time - timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    settlement_date = (
        trade_timestamp.date() + timedelta(days=2)
    )

    trade = {
        "trade_id": f"TRD{number:010d}",
        "account_id": (
            f"ACC{random.randint(1, account_count):08d}"
        ),
        "security_id": (
            f"SEC{random.randint(1, security_count):06d}"
        ),
        "counterparty_id": (
            f"CP{random.randint(1, counterparty_count):06d}"
        ),
        "trade_type": random.choice(["BUY", "SELL"]),
        "quantity": random.randint(1, 1000),
        "trade_price": round(
            random.uniform(5.00, 1000.00),
            2,
        ),
        "trade_currency": random.choice(
            ["USD", "CAD", "GBP"]
        ),
        "trade_timestamp": trade_timestamp.isoformat(),
        "settlement_date": settlement_date.isoformat(),
        "trade_status": random.choice(
            ["EXECUTED", "EXECUTED", "EXECUTED", "CANCELLED"]
        ),
        "source_system": random.choice(
            ["ORDER_MANAGEMENT", "EXECUTION_SYSTEM"]
        ),
        "ingestion_timestamp": current_time.isoformat(),
    }

    trades.append(trade)


# Create the output folder
output_folder = Path("data/generated/trades")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the trade records into a table
trades_table = pa.Table.from_pylist(trades)


# Save the trades as a Parquet file
output_file = output_folder / "trades.parquet"

pq.write_table(
    trades_table,
    output_file,
    compression="snappy",
)


print(f"Generated trades: {len(trades):,}")
print(f"Saved to: {output_file}")