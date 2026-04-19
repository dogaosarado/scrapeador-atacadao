import psycopg2
from datetime import datetime, timezone

from config import DB_CONFIG

WINDOWS = [7, 30, 90]
MIN_SAMPLE = 10


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def compute_and_upsert(conn):
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        for window_days in WINDOWS:
            cur.execute("""
            INSERT INTO volatility_metrics (sku, ean, window_days, stddev_price, mean_price, cv, sample_size, computed_at)
            SELECT
                sku,
                ean,
                %s                                                        AS window_days,
                ROUND(STDDEV_SAMP(price)::NUMERIC, 6)                    AS stddev_price,
                ROUND(AVG(price)::NUMERIC, 6)                            AS mean_price,
                ROUND((STDDEV_SAMP(price) / AVG(price))::NUMERIC, 6)    AS cv,
                COUNT(*)                                                  AS sample_size,
                %s                                                        AS computed_at
            FROM prices
            WHERE
                price IS NOT NULL
                AND timestamp >= NOW() - (%s * INTERVAL '1 day')
            GROUP BY sku, ean
            HAVING COUNT(*) >= %s
            ON CONFLICT (sku, ean, window_days) DO UPDATE SET
                stddev_price = EXCLUDED.stddev_price,
                mean_price   = EXCLUDED.mean_price,
                cv           = EXCLUDED.cv,
                sample_size  = EXCLUDED.sample_size,
                computed_at  = EXCLUDED.computed_at
            """, (window_days, now, window_days, MIN_SAMPLE))

            print(f"  {window_days}d window: {cur.rowcount} SKUs upserted")

    conn.commit()


def main():
    conn = get_conn()
    print("Computing volatility metrics...")
    compute_and_upsert(conn)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()