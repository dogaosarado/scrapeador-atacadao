import sqlite3

conn = sqlite3.connect("atacadao.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_path TEXT UNIQUE,
    level1 TEXT,
    level2 TEXT,
    level3 TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS product_categories (
    sku TEXT,
    ean TEXT,
    category_id INTEGER,
    FOREIGN KEY(category_id) REFERENCES categories(id)
)
""")

conn.commit()
conn.close()

print("Category tables created")