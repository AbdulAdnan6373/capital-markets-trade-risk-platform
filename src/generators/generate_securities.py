import random
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from faker import Faker


# Read the project configuration
with open("config/data_generation.yml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


# Get the development record count
record_count = config["datasets"]["securities"][
    "development_records"
]


# Set up the imaginary-data generators
fake = Faker("en_US")

random.seed(43)
Faker.seed(43)


# Possible values for security information
asset_classes = ["EQUITY", "BOND", "ETF"]

sectors = [
    "TECHNOLOGY",
    "FINANCIALS",
    "HEALTHCARE",
    "ENERGY",
    "CONSUMER",
    "INDUSTRIALS",
]

exchanges = ["NASDAQ", "NYSE", "OTC"]

currencies = ["USD", "CAD", "GBP"]


# Empty list for storing securities
securities = []


# Generate the required number of securities
for number in range(1, record_count + 1):

    asset_class = random.choice(asset_classes)

    security = {
        "security_id": f"SEC{number:06d}",
        "symbol": f"SYM{number:05d}",
        "security_name": fake.company(),
        "asset_class": asset_class,
        "sector": random.choice(sectors),
        "exchange": random.choice(exchanges),
        "currency": random.choice(currencies),
        "active_flag": random.choice(
            ["Y", "Y", "Y", "Y", "N"]
        ),
        "source_system": "SECURITY_MASTER",
        "ingestion_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    securities.append(security)


# Create the output folder
output_folder = Path("data/generated/securities")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the records into a table
securities_table = pa.Table.from_pylist(securities)


# Save the data as a compressed Parquet file
output_file = output_folder / "securities.parquet"

pq.write_table(
    securities_table,
    output_file,
    compression="snappy",
)


print(f"Generated securities: {len(securities):,}")
print(f"Saved to: {output_file}")