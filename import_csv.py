import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load .env file
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

# ✅ Load CSV
csv_path = "questions_clean.csv"
df = pd.read_csv(csv_path)

# ✅ Insert rows
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO questions (question, option_a, option_b, option_c, option_d, answer, rationale)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (
        row['question'],
        row['A'],
        row['B'],
        row['C'],
        row['D'],
        row['Answer'],
        row['Rationale']
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ Data imported successfully!")
