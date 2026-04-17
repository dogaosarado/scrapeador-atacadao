import sqlite3
import json
from datetime import datetime

JSON_FILE = "atacadao_catalog.json"
DB_FILE = "atacadao.db"


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        sku TEXT,
        ean TEXT,
        product_id TEXT,
        name TEXT,
        brand TEXT,
        url TEXT,
        image TEXT,
        first_seen TEXT,
        last_seen TEXT,
        PRIMARY KEY (sku, ean)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT,
        ean TEXT,
        price REAL,
        list_price REAL,
        available BOOLEAN,
        timestamp TEXT,
        run_id INTEGER,
        FOREIGN KEY (sku, ean) REFERENCES products(sku, ean)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT
    )
    """)

    conn.commit()


def insert_data(conn, products):
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Create run entry
    cursor.execute(
        "INSERT INTO runs (timestamp) VALUES (?)",
        (now,)
    )

    run_id = cursor.lastrowid

    for product in products:
        items = product.get("items", [])

        for item in items:
            sku = item.get("sku")
            ean = item.get("ean")

            if not sku:
                continue

            # Insert product if new
            cursor.execute("""
            INSERT OR IGNORE INTO products (
                sku, ean, product_id, name, brand, url, image, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sku,
                ean,
                product.get("product_id"),
                product.get("product_name"),
                product.get("brand"),
                product.get("url"),
                item.get("images", [None])[0],
                now,
                now
            ))

            # Always update last_seen
            cursor.execute("""
            UPDATE products
            SET last_seen = ?
            WHERE sku = ? AND ean = ?
            """, (now, sku, ean))

            # Insert price
            cursor.execute("""
            INSERT INTO prices (
                sku, ean, price, list_price, available, timestamp, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                sku,
                ean,
                item.get("price"),
                item.get("list_price"),
                item.get("available"),
                now,
                run_id
            ))

    conn.commit()


def main():
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = sqlite3.connect(DB_FILE)

    create_tables(conn)
    insert_data(conn, data)

    conn.close()

    print("Database updated")


if __name__ == "__main__":
    main()
