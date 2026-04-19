import os

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "ep-dawn-rain-am7ojcel.c-5.us-east-1.aws.neon.tech"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "neondb"),
    "user":     os.environ.get("DB_USER", "neondb_owner"),
    "password": os.environ.get("DB_PASSWORD", "npg_wOjeC7IvTtr6"),
    "sslmode":  "require"
}