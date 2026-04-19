import json
import psycopg2
from datetime import datetime, timezone

from config import DB_CONFIG

JSON_FILE = "atacadao_catalog.json"


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            sku         TEXT,
            ean         TEXT,
            product_id  TEXT,
            name        TEXT,
            brand       TEXT,
            url         TEXT,
            image       TEXT,
            first_seen  TIMESTAMPTZ,
            last_seen   TIMESTAMPTZ,
            PRIMARY KEY (sku, ean)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id          SERIAL PRIMARY KEY,
            sku         TEXT,
            ean         TEXT,
            price       NUMERIC,
            list_price  NUMERIC,
            available   BOOLEAN,
            timestamp   TIMESTAMPTZ,
            run_id      INTEGER,
            FOREIGN KEY (sku, ean) REFERENCES products(sku, ean)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id          SERIAL PRIMARY KEY,
            timestamp   TIMESTAMPTZ
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id              SERIAL PRIMARY KEY,
            category_path   TEXT UNIQUE,
            level1          TEXT,
            level2          TEXT,
            level3          TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_categories (
            sku         TEXT,
            ean         TEXT,
            category_id INTEGER REFERENCES categories(id),
            PRIMARY KEY (sku, ean, category_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS price_changes (
            id              SERIAL PRIMARY KEY,
            sku             TEXT,
            ean             TEXT,
            old_price       NUMERIC,
            new_price       NUMERIC,
            change_percent  NUMERIC,
            timestamp       TIMESTAMPTZ
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS volatility_metrics (
            sku          TEXT,
            ean          TEXT,
            window_days  INTEGER,
            stddev_price NUMERIC,
            mean_price   NUMERIC,
            cv           NUMERIC,
            sample_size  INTEGER,
            computed_at  TIMESTAMPTZ,
            PRIMARY KEY (sku, ean, window_days)
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
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO runs (timestamp) VALUES (%s) RETURNING id", (now,))
        run_id = cur.fetchone()[0]

        for product in products:
            categories = product.get("categories", [])

            for item in product.get("items", []):
                sku = item.get("sku")
                ean = item.get("ean")

                if not sku:
                    continue

                cur.execute("""
                INSERT INTO products (sku, ean, product_id, name, brand, url, image, first_seen, last_seen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sku, ean) DO UPDATE SET last_seen = EXCLUDED.last_seen
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

                for category_path in categories:
                    level1, level2, level3 = parse_category(category_path)

                    cur.execute("""
                    INSERT INTO categories (category_path, level1, level2, level3)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (category_path) DO NOTHING
                    """, (category_path, level1, level2, level3))

                    cur.execute("SELECT id FROM categories WHERE category_path = %s", (category_path,))
                    category_id = cur.fetchone()[0]

                    cur.execute("""
                    INSERT INTO product_categories (sku, ean, category_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """, (sku, ean, category_id))

                # price change detection
                cur.execute("""
                SELECT price FROM prices
                WHERE sku = %s AND ean = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """, (sku, ean))

                row = cur.fetchone()
                new_price = item.get("price")

                if row:
                    old_price = float(row[0])
                    if old_price != new_price and old_price is not None and new_price is not None:
                        change_percent = ((new_price - old_price) / old_price) * 100
                        cur.execute("""
                        INSERT INTO price_changes (sku, ean, old_price, new_price, change_percent, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """, (sku, ean, old_price, new_price, change_percent, now))

                cur.execute("""
                INSERT INTO prices (sku, ean, price, list_price, available, timestamp, run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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

    conn = get_conn()
    create_tables(conn)
    insert_data(conn, data)
    conn.close()

    print("Database updated")


if __name__ == "__main__":
    main()
