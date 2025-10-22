import psycopg2
from dotenv import load_dotenv
import os
import csv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# ✅ Connect to PostgreSQL
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# ✅ Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS questions (
    id SERIAL PRIMARY KEY,
    question TEXT,
    option_a TEXT,
    option_b TEXT,
    option_c TEXT,
    option_d TEXT,
    answer TEXT,
    rationale TEXT
);
""")
conn.commit()

csv_path = "questions_clean.csv"

# ✅ Read and insert row by row (low memory usage)
with open(csv_path, encoding="utf-8") as file:
    reader = csv.DictReader(file)
    batch = []
    batch_size = 200  # adjust if needed

    for i, row in enumerate(reader, start=1):
        batch.append((
            row.get('question'),
            row.get('A'),
            row.get('B'),
            row.get('C'),
            row.get('D'),
            row.get('Answer'),
            row.get('Rationale')
        ))

        if len(batch) >= batch_size:
            cursor.executemany("""
                INSERT INTO questions (question, option_a, option_b, option_c, option_d, answer, rationale)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, batch)
            conn.commit()
            print(f"Inserted {i} rows...")
            batch = []

    # Final remaining rows
    if batch:
        cursor.executemany("""
            INSERT INTO questions (question, option_a, option_b, option_c, option_d, answer, rationale)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, batch)
        conn.commit()

print("✅ All data imported successfully (streaming mode)!")
cursor.close()
conn.close()
