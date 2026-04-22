import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "ep-frosty-water-am38vdoe-pooler.c-5.us-east-1.aws.neon.tech"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "neondb"),
        user=os.environ.get("DB_USER", "neondb_owner"),
        password=os.environ.get("DB_PASSWORD", "Atacadao2025"),
        sslmode="require"
    )


@app.get("/products")
def get_products():
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT sku, ean, name, brand, url, image
        FROM products
        LIMIT 20
        """)
        rows = cur.fetchall()
    conn.close()
    return list(rows)

@app.get("/products/search")
def search_products(q: str = "", limit: int = 20):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT DISTINCT ON (p.sku, p.ean)
            p.sku, p.ean, p.name, p.brand, p.url, p.image,
            pr.price, pr.list_price, pr.available
        FROM products p
        LEFT JOIN (
            SELECT DISTINCT ON (sku, ean) sku, ean, price, list_price, available
            FROM prices
            ORDER BY sku, ean, timestamp DESC
        ) pr ON p.sku = pr.sku AND p.ean = pr.ean
        WHERE p.name ILIKE %s OR p.brand ILIKE %s
        LIMIT %s
        """, (f"%{q}%", f"%{q}%", limit))
        rows = cur.fetchall()
    conn.close()
    return list(rows)

@app.get("/products/{sku}/history")
def get_price_history(sku: str):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT price, list_price, available, timestamp
        FROM prices
        WHERE sku = %s
        ORDER BY timestamp ASC
        """, (sku,))
        rows = cur.fetchall()
    conn.close()
    return list(rows)


@app.get("/price-changes")
def get_price_changes(limit: int = 50):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT pc.sku, pc.ean, p.name, p.brand, p.image, p.url,
               pc.old_price, pc.new_price, pc.change_percent, pc.timestamp
        FROM price_changes pc
        JOIN products p ON pc.sku = p.sku AND pc.ean = p.ean
        ORDER BY pc.timestamp DESC
        LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return list(rows)


@app.get("/volatility")
def get_volatility(window_days: int = 30, limit: int = 50):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT vm.sku, vm.ean, p.name, p.brand, p.image, p.url,
               vm.stddev_price, vm.mean_price, vm.cv, vm.sample_size
        FROM volatility_metrics vm
        JOIN products p ON vm.sku = p.sku AND vm.ean = p.ean
        WHERE vm.window_days = %s
        ORDER BY vm.cv DESC
        LIMIT %s
        """, (window_days, limit))
        rows = cur.fetchall()
    conn.close()
    return list(rows)

@app.get("/volatility/sku")
def get_volatility_by_sku(sku: str, ean: str):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT window_days, stddev_price, mean_price, cv, sample_size
        FROM volatility_metrics
        WHERE sku = %s AND ean = %s
        ORDER BY window_days ASC
        """, (sku, ean))
        rows = cur.fetchall()
    conn.close()
    return list(rows)

@app.get("/price-changes")
def get_price_changes(limit: int = 50):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
        SELECT pc.sku, pc.ean, p.name, p.brand, p.image, p.url,
               pc.old_price, pc.new_price, pc.change_percent, pc.timestamp
        FROM price_changes pc
        JOIN products p ON pc.sku = p.sku AND pc.ean = p.ean
        ORDER BY pc.timestamp DESC
        LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
    conn.close()
    return list(rows)