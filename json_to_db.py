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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT,
        ean TEXT,
        old_price REAL,
        new_price REAL,
        change_percent REAL,
        timestamp TEXT
    )
    """)

    conn.commit()


def parse_category(category_path):
    parts = category_path.strip("/").split("/")

    level1 = parts[0] if len(parts) > 0 else None
    level2 = parts[1] if len(parts) > 1 else None
    level3 = parts[2] if len(parts) > 2 else None

    return level1, level2, level3


def insert_data(conn, products):
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # create run
    cursor.execute(
        "INSERT INTO runs (timestamp) VALUES (?)",
        (now,)
    )

    run_id = cursor.lastrowid

    for product in products:

        categories = product.get("categories", [])

        items = product.get("items", [])

        for item in items:

            sku = item.get("sku")
            ean = item.get("ean")

            if not sku:
                continue

            # insert product
            cursor.execute("""
            INSERT OR IGNORE INTO products (
                sku, ean, product_id, name, brand, url, image,
                first_seen, last_seen
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

            # update last seen
            cursor.execute("""
            UPDATE products
            SET last_seen = ?
            WHERE sku = ? AND ean = ?
            """, (now, sku, ean))

            # categories
            for category_path in categories:

                level1, level2, level3 = parse_category(category_path)

                cursor.execute("""
                INSERT OR IGNORE INTO categories (
                    category_path, level1, level2, level3
                ) VALUES (?, ?, ?, ?)
                """, (
                    category_path,
                    level1,
                    level2,
                    level3
                ))

                cursor.execute("""
                SELECT id FROM categories
                WHERE category_path = ?
                """, (category_path,))

                category_id = cursor.fetchone()[0]

                cursor.execute("""
                INSERT OR IGNORE INTO product_categories (
                    sku, ean, category_id
                ) VALUES (?, ?, ?)
                """, (
                    sku,
                    ean,
                    category_id
                ))

            # price change detection
            cursor.execute("""
            SELECT price FROM prices
            WHERE sku = ? AND ean = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """, (sku, ean))

            row = cursor.fetchone()

            new_price = item.get("price")

            if row:
                old_price = row[0]

                if old_price != new_price and old_price is not None and new_price is not None:
                    change_percent = ((new_price - old_price) / old_price) * 100

                    cursor.execute("""
                    INSERT INTO price_changes (
                        sku, ean, old_price, new_price,
                        change_percent, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        sku,
                        ean,
                        old_price,
                        new_price,
                        change_percent,
                        now
                    ))

            # insert price
            cursor.execute("""
            INSERT INTO prices (
                sku, ean, price, list_price,
                available, timestamp, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                sku,
                ean,
                new_price,
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
