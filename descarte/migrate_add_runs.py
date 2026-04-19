import sqlite3

conn = sqlite3.connect("atacadao.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT
)
""")

conn.commit()
conn.close()

print("Runs table created")