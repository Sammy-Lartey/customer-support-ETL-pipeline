from libs import *


load_dotenv()

CONFIG = {
    # File Paths
    "path": os.getenv("CS2025_DATA_PATH"),
    "excel_file": os.getenv("CS2025_EXCEL_FILE"),

    # DB Schema
    "schema": os.getenv("CS2025_SCHEMA"),

    # DB Credentials
    "db_credentials": {
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_PORT": os.getenv("DB_PORT"),
    },

    "logging_level": os.getenv("LOG_LEVEL"),
    "exclude_sheets": os.getenv("CS2025_EXCLUDE_SHEETS").split(","),
}


for key, val in CONFIG["db_credentials"].items():
    if val is None and key not in ("DB_PORT",):
        raise ValueError(f"Missing required DB credential: {key}")