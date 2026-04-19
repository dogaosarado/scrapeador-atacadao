import aiohttp
import asyncio
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

BASE_URL = "https://www.atacadao.com.br/api/io/_v/api/intelligent-search/product_search"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

CONCURRENT_REQUESTS = 5
PAGE_SIZE = 50


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "ep-frosty-water-am38vdoe-pooler.c-5.us-east-1.aws.neon.tech"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "neondb"),
        user=os.environ.get("DB_USER", "neondb_owner"),
        password=os.environ.get("DB_PASSWORD", "Atacadao2024"),
        sslmode="require"
    )


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


def extract(product):
    try:
        skus = []

        for item in product.get("items", []):
            seller = item.get("sellers", [{}])[0]
            offer = seller.get("commertialOffer", {})

            skus.append({
                "sku": item.get("itemId"),
                "ean": item.get("ean"),
                "name": item.get("name"),
                "price": offer.get("Price"),
                "list_price": offer.get("ListPrice"),
                "available": offer.get("IsAvailable"),
                "images": [img.get("imageUrl") for img in item.get("images", [])],
            })

        return {
            "product_id": product.get("productId"),
            "product_name": product.get("productName"),
            "brand": product.get("brand"),
            "url": f"https://www.atacadao.com.br/{product.get('linkText')}/p",
            "categories": product.get("categories", []),
            "items": skus,
        }

    except Exception:
        return None


async def fetch_page(session, page):
    params = {
        "q": "",
        "from": page * PAGE_SIZE,
        "to": (page + 1) * PAGE_SIZE - 1
    }

    try:
        async with session.get(BASE_URL, headers=HEADERS, params=params) as response:
            if response.status != 200:
                return []
            data = await response.json()
            return data.get("products", [])
    except Exception:
        return []


def insert_batch(conn, products, run_id, now, last_prices, category_map):
    all_categories = set()
    for product in products:
        for cat in product.get("categories", []):
            all_categories.add(cat)

    with conn.cursor() as cur:
        # upsert new categories
        if all_categories:
            new_cats = [c for c in all_categories if c not in category_map]
            if new_cats:
                psycopg2.extras.execute_values(cur, """
                INSERT INTO categories (category_path, level1, level2, level3)
                VALUES %s
                ON CONFLICT (category_path) DO NOTHING
                """, [(cat, *parse_category(cat)) for cat in new_cats])

                cur.execute("SELECT category_path, id FROM categories WHERE category_path = ANY(%s)", (new_cats,))
                for row in cur.fetchall():
                    category_map[row[0]] = row[1]

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
                    sku, ean,
                    product.get("product_id"),
                    product.get("product_name"),
                    product.get("brand"),
                    product.get("url"),
                    item.get("images", [None])[0],
                    now, now
                ))

                price_rows.append((sku, ean, new_price, item.get("list_price"), item.get("available"), now, run_id))

                old_price = last_prices.get((sku, ean))
                if old_price is not None and new_price is not None and old_price != new_price:
                    change_percent = ((new_price - old_price) / old_price) * 100
                    price_change_rows.append((sku, ean, old_price, new_price, change_percent, now))

                for cat in categories:
                    cat_id = category_map.get(cat)
                    if cat_id:
                        product_category_rows.append((sku, ean, cat_id))

        if product_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO products (sku, ean, product_id, name, brand, url, image, first_seen, last_seen)
            VALUES %s
            ON CONFLICT (sku, ean) DO UPDATE SET last_seen = EXCLUDED.last_seen
            """, product_rows)

        if price_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO prices (sku, ean, price, list_price, available, timestamp, run_id)
            VALUES %s
            """, price_rows)

        if price_change_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO price_changes (sku, ean, old_price, new_price, change_percent, timestamp)
            VALUES %s
            """, price_change_rows)

        if product_category_rows:
            psycopg2.extras.execute_values(cur, """
            INSERT INTO product_categories (sku, ean, category_id)
            VALUES %s
            ON CONFLICT DO NOTHING
            """, product_category_rows)

    conn.commit()
    return len(product_rows)


async def scrape():
    conn = get_conn()
    create_tables(conn)

    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute("INSERT INTO runs (timestamp) VALUES (%s) RETURNING id", (now,))
        run_id = cur.fetchone()[0]

        cur.execute("""
        SELECT DISTINCT ON (sku, ean) sku, ean, price
        FROM prices
        ORDER BY sku, ean, timestamp DESC
        """)
        last_prices = {(row[0], row[1]): float(row[2]) for row in cur.fetchall() if row[2] is not None}

        cur.execute("SELECT category_path, id FROM categories")
        category_map = {row[0]: row[1] for row in cur.fetchall()}

    conn.commit()

    total = 0
    seen_ids = set()
    page = 0

    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_page(session, page + i) for i in range(CONCURRENT_REQUESTS)]
            pages = await asyncio.gather(*tasks)

            stop = True
            batch = []

            for products in pages:
                if products:
                    stop = False
                for p in products:
                    data = extract(p)
                    if data and data["product_id"] not in seen_ids:
                        seen_ids.add(data["product_id"])
                        batch.append(data)

            if batch:
                inserted = insert_batch(conn, batch, run_id, now, last_prices, category_map)
                total += inserted
                print(f"Scraped and inserted {total} SKUs so far")

            if stop:
                break

            page += CONCURRENT_REQUESTS

    conn.close()
    print(f"Done. Total SKUs inserted: {total}")


if __name__ == "__main__":
    asyncio.run(scrape())