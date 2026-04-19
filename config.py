import os

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "ep-frosty-water-am38vdoe-pooler.c-5.us-east-1.aws.neon.tech"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "neondb"),
    "user":     os.environ.get("DB_USER", "neondb_owner"),
    "password": os.environ.get("DB_PASSWORD", "AbZInho22344432"),
    "sslmode":  "require"
<<<<<<< Updated upstream
}
=======
}
>>>>>>> Stashed changes
