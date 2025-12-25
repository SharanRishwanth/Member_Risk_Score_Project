import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()

OUTPUT_DIR = "C:/Users/A/Documents/Member Risk Project/DATA/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------------------------------
# 1) MEMBERS DATA
# -----------------------------------------------------
def generate_members(n=100):
    data = []

    for i in range(1, n + 1):
        data.append({
            "member_id": i,
            "name": fake.name(),
            "age": random.randint(18, 80),
            "gender": random.choice(["M", "F"]),
            "city": fake.city(),

            # NEW COLUMNS
            "join_date": fake.date_between(start_date="-5y", end_date="today"),
            "member_status": random.choice(["Active", "Inactive"]),
            "income_bracket": random.choice(["Low", "Medium", "High"])
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/csv/members.csv", index=False)
    df.to_parquet(f"{OUTPUT_DIR}/parquet/members.parquet", index=False)

    print("✔ members.csv & members.parquet created")
    return df


# -----------------------------------------------------
# 2) VISITS DATA (HEALTHCARE)
# -----------------------------------------------------
def generate_visits(members_df, n=350):
    data = []
    diagnoses = ["Diabetes", "Hypertension", "Asthma", "Flu", "Migraine", "COVID"]
    providers = ["City Hospital", "Metro Clinic", "HealthPlus Center", "Apex Hospital", "Sunrise Medical"]

    for i in range(1, n + 1):
        member = members_df.sample(1).iloc[0]
        billed = random.randint(200, 20000)
        paid = billed - random.randint(0, billed // 3)

        data.append({
            "visit_id": i,
            "member_id": member["member_id"],
            "date": fake.date_between(start_date="-1y", end_date="today"),
            "diagnosis": random.choice(diagnoses),
            "billed_amt": billed,
            "paid_amt": paid,

            # NEW COLUMNS
            "provider": random.choice(providers),
            "is_emergency": random.choice([0, 1]),
            "claim_status": random.choice(["Paid", "Pending", "Denied"])
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/csv/visits.csv", index=False)
    df.to_parquet(f"{OUTPUT_DIR}/parquet/visits.parquet", index=False)

    print("✔ visits.csv & visits.parquet created")
    return df


# -----------------------------------------------------
# 3) TRANSACTIONS DATA (FINANCE)
# -----------------------------------------------------
def generate_transactions(members_df, n=500):
    data = []
    categories = ["Food", "Travel", "Shopping", "Healthcare", "Rent", "Utilities", "Fraud-Suspect"]
    payment_methods = ["Card", "UPI", "NetBanking", "Cash"]
    merchants = ["Amazon", "Flipkart", "Swiggy", "Zomato", "Uber", "BigBazaar", "Apollo Pharmacy","suspect","unknown"]

    for i in range(1, n + 1):
        member = members_df.sample(1).iloc[0]

        data.append({
            "txn_id": i,
            "member_id": member["member_id"],
            "txn_date": fake.date_between(start_date="-1y", end_date="today"),
            "category": random.choice(categories),
            "amount": round(random.uniform(10, 10000), 2),

            # NEW COLUMNS
            "payment_method": random.choice(payment_methods),
            "merchant": random.choice(merchants),
            "is_international": random.choice([0, 1])
        })

    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/csv/transactions.csv", index=False)
    df.to_parquet(f"{OUTPUT_DIR}/parquet/transactions.parquet", index=False)

    print("✔ transactions.csv & transactions.parquet created")
    return df


# -----------------------------------------------------
# MAIN EXECUTION
# -----------------------------------------------------
if __name__ == "__main__":
    print("Generating enriched dummy data...")

    members_df = generate_members(100)
    visits_df = generate_visits(members_df, 350)
    txns_df = generate_transactions(members_df, 500)

    print("\n All enriched CSV + Parquet files generated in:", OUTPUT_DIR)
