import json
import psycopg2
import psycopg2.extras
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
        # create run
        cur.execute("INSERT INTO runs (timestamp) VALUES (%s) RETURNING id", (now,))
        run_id = cur.fetchone()[0]

        # collect all unique categories and bulk upsert
        all_categories = set()
        for product in products:
            for cat in product.get("categories", []):
                all_categories.add(cat)

        if all_categories:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO categories (category_path, level1, level2, level3)
            VALUES %s
            ON CONFLICT (category_path) DO NOTHING
            """, [
                (cat, *parse_category(cat))
                for cat in all_categories
            ])

        # fetch all category ids into a local dict — no per-row SELECT
        cur.execute("SELECT category_path, id FROM categories")
        category_map = {row[0]: row[1] for row in cur.fetchall()}

        # fetch last known prices for change detection — one query, not per-row
        cur.execute("""
        SELECT DISTINCT ON (sku, ean) sku, ean, price
        FROM prices
        ORDER BY sku, ean, timestamp DESC
        """)
        last_prices = {(row[0], row[1]): float(row[2]) for row in cur.fetchall() if row[2] is not None}

        # collect rows for bulk insert
        product_rows = []
        price_rows = []
        price_change_rows = []
        product_category_rows = []

        for product in products:
            categories = product.get("categories", [])

            for item in product.get("items", []):
                sku = item.get("sku")
                ean = item.get("ean")

                if not sku:
                    continue

                new_price = item.get("price")

                product_rows.append((
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

                price_rows.append((
                    sku,
                    ean,
                    new_price,
                    item.get("list_price"),
                    item.get("available"),
                    now,
                    run_id
                ))

                # price change detection using local dict
                old_price = last_prices.get((sku, ean))
                if old_price is not None and new_price is not None and old_price != new_price:
                    change_percent = ((new_price - old_price) / old_price) * 100
                    price_change_rows.append((sku, ean, old_price, new_price, change_percent, now))

                for cat in categories:
                    cat_id = category_map.get(cat)
                    if cat_id:
                        product_category_rows.append((sku, ean, cat_id))

        # bulk insert products
        if product_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO products (sku, ean, product_id, name, brand, url, image, first_seen, last_seen)
            VALUES %s
            ON CONFLICT (sku, ean) DO UPDATE SET last_seen = EXCLUDED.last_seen
            """, product_rows)

        # bulk insert prices
        if price_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO prices (sku, ean, price, list_price, available, timestamp, run_id)
            VALUES %s
            """, price_rows)

        # bulk insert price changes
        if price_change_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO price_changes (sku, ean, old_price, new_price, change_percent, timestamp)
            VALUES %s
            """, price_change_rows)

        # bulk insert product categories
        if product_category_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO product_categories (sku, ean, category_id)
            VALUES %s
            ON CONFLICT DO NOTHING
            """, product_category_rows)

    conn.commit()
    print(f"Inserted {len(product_rows)} SKUs, {len(price_rows)} price rows, {len(price_change_rows)} price changes")


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