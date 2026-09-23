import random
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from faker import Faker


# Read the project configuration file
with open("config/data_generation.yml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


# Get the number of development records from the configuration
record_count = config["datasets"]["accounts"]["development_records"]


# Create tools for generating imaginary data
fake = Faker("en_US")

random.seed(42)
Faker.seed(42)


# Create an empty list for storing accounts
accounts = []


# Repeat the account creation process 1,000 times
for number in range(1, record_count + 1):

    account_type = random.choices(
        ["INDIVIDUAL", "INSTITUTIONAL"],
        weights=[70, 30],
        k=1,
    )[0]

    if account_type == "INDIVIDUAL":
        customer_name = fake.name()
    else:
        customer_name = fake.company()

    account = {
        "account_id": f"ACC{number:08d}",
        "customer_name": customer_name,
        "account_type": account_type,
        "country": random.choice(["US", "CA", "GB", "IN"]),
        "base_currency": random.choice(["USD", "CAD", "GBP"]),
        "risk_rating": random.choice(["LOW", "MEDIUM", "HIGH"]),
        "account_status": random.choice(
            ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"]
        ),
        "source_system": "CUSTOMER_MASTER",
        "ingestion_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    accounts.append(account)


# Create the output folder if it does not already exist
output_folder = Path("data/generated/accounts")
output_folder.mkdir(parents=True, exist_ok=True)


# Convert the Python list into a table
accounts_table = pa.Table.from_pylist(accounts)


# Save the table as a compressed Parquet file
output_file = output_folder / "accounts.parquet"

pq.write_table(
    accounts_table,
    output_file,
    compression="snappy",
)


print(f"Generated accounts: {len(accounts):,}")
print(f"Saved to: {output_file}")