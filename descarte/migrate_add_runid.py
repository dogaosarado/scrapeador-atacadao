import sqlite3

conn = sqlite3.connect("atacadao.db")
cursor = conn.cursor()

cursor.execute("""
ALTER TABLE prices ADD COLUMN run_id INTEGER
""")

conn.commit()
conn.close()

print("run_id added")