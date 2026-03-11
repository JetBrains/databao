# -------------------------------------------------------------------
# Benchmark configuration
# Edit these constants to match your setup.
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Database connection
# Set DATABASE_TYPE and fill in the relevant section below.
# -------------------------------------------------------------------

# Choose your database type: "snowflake", "duckdb", or "sqlalchemy"
DATABASE_TYPE = "sqlalchemy"

# --- SQLAlchemy (Postgres, MySQL, SQLite, BigQuery, etc.) ---
# Only used when DATABASE_TYPE = "sqlalchemy"
# Examples:
#   "postgresql://user:pass@host:5432/dbname"
#   "mysql+pymysql://user:pass@host:3306/dbname"
#   "sqlite:///path/to/db.sqlite"
#   "bigquery://project/dataset"
DATABASE_URL = "sqlite:///:memory:"  # TODO: replace with your connection string

# --- DuckDB ---
# Only used when DATABASE_TYPE = "duckdb"
DUCKDB_PATH = ""  # path to your .duckdb file

# --- Snowflake ---
# Only used when DATABASE_TYPE = "snowflake"
# Install Snowflake support: pip install -e ".[snowflake]"
SNOWFLAKE_AUTH = "password"  # "password", "key_pair", or "sso"
SNOWFLAKE_USER = ""
SNOWFLAKE_ACCOUNT = ""  # e.g. "xy12345.us-east-1"
SNOWFLAKE_DATABASE = ""
SNOWFLAKE_SCHEMA = ""
SNOWFLAKE_WAREHOUSE = ""  # optional
SNOWFLAKE_PASSWORD = ""  # for "password" auth
SNOWFLAKE_PRIVATE_KEY_PATH = ""  # for "key_pair" auth (path to PEM file)

# -------------------------------------------------------------------
# Benchmark settings
# -------------------------------------------------------------------

# Path to CSV with gold SQL queries (columns: user_input, gold_sql, difficulty)
INPUT_CSV = "benchmark_questions.csv"

# Where to write benchmark results
OUTPUT_CSV = "results/output.csv"

# OpenAI model used by the LLM judge that evaluates results
JUDGE_MODEL = "gpt-5.4"

# Maximum number of concurrent benchmark queries
MAX_CONCURRENT = 8

# Name for the Ragas dataset (used in experiment tracking)
DATASET_NAME = "my_benchmark"

# -------------------------------------------------------------------
# LangSmith (optional) -- set these to enable tracing
# Install langsmith: pip install -e ".[langsmith]"
# -------------------------------------------------------------------
# LANGSMITH_API_KEY = "lsv2_..."
# LANGSMITH_PROJECT = "my-benchmark"
