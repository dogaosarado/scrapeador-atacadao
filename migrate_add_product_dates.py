import sqlite3

conn = sqlite3.connect("atacadao.db")
cursor = conn.cursor()

cursor.execute("""
ALTER TABLE products ADD COLUMN first_seen TEXT
""")

cursor.execute("""
ALTER TABLE products ADD COLUMN last_seen TEXT
""")

conn.commit()
conn.close()

print("Migration complete")