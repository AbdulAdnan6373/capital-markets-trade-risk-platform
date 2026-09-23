import random
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


# Open and read the project configuration
with open("config/data_generation.yml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


# Read the development record counts
position_count = config["datasets"]["positions"][
    "development_records"
]

account_count = config["datasets"]["accounts"][
    "development_records"
]

security_count = config["datasets"]["securities"][
    "development_records"
]


# Generate repeatable random data
random.seed(47)


# Empty list for storing positions
positions = []


# Store used account and security combinations
used_pairs = set()


# Continue until we have 10,000 unique positions
while len(positions) < position_count:

    account_number = random.randint(1, account_count)
    security_number = random.randint(1, security_count)

    account_security_pair = (
        account_number,
        security_number,
    )

    # Skip the combination if it was already used
    if account_security_pair in used_pairs:
        continue

    used_pairs.add(account_security_pair)

    position_type = random.choice(["LONG", "SHORT"])

    quantity = random.randint(1, 5000)

    if position_type == "SHORT":
        quantity = -quantity

    position_number = len(positions) + 1

    position = {
        "position_id": f"POS{position_number:08d}",
        "account_id": f"ACC{account_number:08d}",
        "security_id": f"SEC{security_number:06d}",
        "position_type": position_type,
        "position_quantity": quantity,
        "average_cost": round(
            random.uniform(5.00, 1000.00),
            2,
        ),
        "position_date": date.today().isoformat(),
        "source_system": "POSITION_KEEPING",
        "ingestion_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    positions.append(position)


# Create the output folder
output_folder = Path("data/generated/positions")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the records into a table
positions_table = pa.Table.from_pylist(positions)


# Save the data as Parquet
output_file = output_folder / "positions.parquet"

pq.write_table(
    positions_table,
    output_file,
    compression="snappy",
)


print(f"Generated positions: {len(positions):,}")
print(f"Saved to: {output_file}")