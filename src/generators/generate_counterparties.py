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


# Read the number of counterparties to generate
record_count = config["datasets"]["counterparties"][
    "development_records"
]


# Set up the imaginary-data generators
fake = Faker("en_US")

random.seed(44)
Faker.seed(44)


# Possible counterparty values
counterparty_types = [
    "BANK",
    "BROKER_DEALER",
    "ASSET_MANAGER",
    "CLEARING_HOUSE",
]

credit_ratings = ["AAA", "AA", "A", "BBB", "BB"]

countries = ["US", "CA", "GB", "DE", "JP"]


# Empty list for storing counterparties
counterparties = []


# Generate the counterparty records
for number in range(1, record_count + 1):

    counterparty = {
        "counterparty_id": f"CP{number:06d}",
        "counterparty_name": fake.company(),
        "counterparty_type": random.choice(
            counterparty_types
        ),
        "country": random.choice(countries),
        "credit_rating": random.choice(credit_ratings),
        "risk_limit_usd": round(
            random.uniform(1_000_000, 50_000_000),
            2,
        ),
        "counterparty_status": random.choice(
            ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]
        ),
        "source_system": "COUNTERPARTY_MASTER",
        "ingestion_timestamp": datetime.now(
    timezone.utc
).isoformat(),
    }

    counterparties.append(counterparty)


# Create the output folder
output_folder = Path("data/generated/counterparties")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the Python records into a table
counterparties_table = pa.Table.from_pylist(
    counterparties
)


# Save the data as a Parquet file
output_file = output_folder / "counterparties.parquet"

pq.write_table(
    counterparties_table,
    output_file,
    compression="snappy",
)


print(f"Generated counterparties: {len(counterparties):,}")
print(f"Saved to: {output_file}")