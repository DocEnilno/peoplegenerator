import sqlite3
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from tqdm import tqdm

fake = Faker()
DB_FILE = "synthetic_people.db"
TOTAL_PEOPLE = 100_000_000
BATCH_SIZE = 5000

def setup_database():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            birth_date TEXT,
            age INTEGER,
            income REAL,
            email TEXT,
            phone TEXT,
            address TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS bank_accounts (
            id TEXT PRIMARY KEY,
            person_id TEXT,
            bank_name TEXT,
            iban TEXT,
            balance REAL,
            open_date TEXT,
            FOREIGN KEY(person_id) REFERENCES persons(id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS relationships (
            id TEXT PRIMARY KEY,
            person_id TEXT,
            relative_id TEXT,
            relation_type TEXT,
            FOREIGN KEY(person_id) REFERENCES persons(id),
            FOREIGN KEY(relative_id) REFERENCES persons(id)
        )
    ''')

    conn.commit()
    conn.close()

def generate_person():
    birth_date = fake.date_of_birth(minimum_age=0, maximum_age=100)
    age = (datetime.now().date() - birth_date).days // 365
    return {
        "id": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "birth_date": birth_date.isoformat(),
        "age": age,
        "income": round(random.uniform(10_000, 200_000), 2),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address().replace("\n", ", ")
    }

def generate_bank_account(person_id):
    return {
        "id": str(uuid.uuid4()),
        "person_id": person_id,
        "bank_name": fake.company(),
        "iban": fake.iban(),
        "balance": round(random.uniform(500, 100_000), 2),
        "open_date": fake.date_between(start_date='-10y', end_date='today').isoformat()
    }

def insert_batch(conn, people, accounts):
    cur = conn.cursor()
    cur.executemany('''
        INSERT INTO persons (id, first_name, last_name, birth_date, age, income, email, phone, address)
        VALUES (:id, :first_name, :last_name, :birth_date, :age, :income, :email, :phone, :address)
    ''', people)

    cur.executemany('''
        INSERT INTO bank_accounts (id, person_id, bank_name, iban, balance, open_date)
        VALUES (:id, :person_id, :bank_name, :iban, :balance, :open_date)
    ''', accounts)

    conn.commit()

def main():
    setup_database()
    conn = sqlite3.connect(DB_FILE)
    pbar = tqdm(total=TOTAL_PEOPLE, desc="Generating")  # Optional

    for _ in range(0, TOTAL_PEOPLE, BATCH_SIZE):
        people = [generate_person() for _ in range(BATCH_SIZE)]
        accounts = [generate_bank_account(p["id"]) for p in people]
        insert_batch(conn, people, accounts)
        pbar.update(BATCH_SIZE)

    conn.close()
    pbar.close()

if __name__ == "__main__":
    main()
