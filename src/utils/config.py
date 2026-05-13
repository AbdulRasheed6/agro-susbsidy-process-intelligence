import os
from pathlib import Path
from dotenv import load_dotenv
import socket

# =========================================================
# 🔹 PROJECT ROOT (robust resolution)
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# =========================================================
# 🔹 VALID STAGES
# =========================================================
VALID_STAGES = {"admin", "ingestion", "transform", "analytics"}

# =========================================================
# 🔹 RESOLVE PIPELINE STAGE (STRICT + FLEXIBLE)
# =========================================================
PIPELINE_STAGE = os.getenv("PIPELINE_STAGE")

# Optional fallback (useful for tests/scripts)
if not PIPELINE_STAGE:
    # Infer from script name if possible
    import sys
    script_name = Path(sys.argv[0]).name.lower()

    if "ingestion" in script_name:
        PIPELINE_STAGE = "ingestion"
    elif "transform" in script_name:
        PIPELINE_STAGE = "transform"
    elif "analytics" in script_name:
        PIPELINE_STAGE = "analytics"
    elif "admin" in script_name:
        PIPELINE_STAGE = "admin"

# Final validation
if not PIPELINE_STAGE:
    raise ValueError(
        "PIPELINE_STAGE is not set and could not be inferred.\n"
        "Set it explicitly: admin | ingestion | transform | analytics"
    )

PIPELINE_STAGE = PIPELINE_STAGE.lower()

if PIPELINE_STAGE not in VALID_STAGES:
    raise ValueError(f"Invalid PIPELINE_STAGE: {PIPELINE_STAGE}")

# =========================================================
# 🔹 LOAD ENV FILE BASED ON STAGE
# =========================================================
env_file_map = {
    "admin": ".env.admin",
    "ingestion": ".env.ingestion",
    "transform": ".env.transform",
    "analytics": ".env.analytics",
}

env_filename = env_file_map[PIPELINE_STAGE]
env_path = PROJECT_ROOT / "env" / env_filename

if not env_path.exists():
    raise FileNotFoundError(f"Missing env file: {env_path}")

load_dotenv(dotenv_path=env_path, override=True)

print(f"[CONFIG] Stage: {PIPELINE_STAGE}")
print(f"[CONFIG] Loaded env: {env_path}")

# =========================================================
# 🔹 STRICT ENV VALIDATION
# =========================================================
def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value

# =========================================================
# 🔹 GENERAL SETTINGS
# =========================================================
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "TEXT").upper()

# =========================================================
# 🔹 DIRECTORIES (safe creation)
# =========================================================
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = DATA_DIR / "logs"
RAW_DIR = PROJECT_ROOT / "temp"

for directory in [DATA_DIR, RAW_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# =========================================================
# 🔹 SPARK & NETWORK CONFIGURATION
# =========================================================
# CRITICAL: This is what tells the Worker how to find the Driver

def best_ip():
    try:
        s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 53))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


SPARK_DRIVER_HOST = ( os.getenv("SPARK_DRIVER_HOST") or best_ip() or socket.gethostname())
SPARK_DRIVER_PORT = os.getenv("SPARK_DRIVER_PORT", "34567")
SPARK_BLOCK_MANAGER_PORT = os.getenv("SPARK_BLOCK_MANAGER_PORT", "34568")

# =========================================================
# 🔹 MINIO CONFIGURATION (STRICT)
# =========================================================
MINIO_ENDPOINT = require_env("MINIO_ENDPOINT")
DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST")
MINIO_URL= require_env("MINIO_URL")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Role-based credentials
if PIPELINE_STAGE == "admin":
    MINIO_ACCESS_KEY = require_env("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = require_env("MINIO_ROOT_PASSWORD")
else:
    prefix = PIPELINE_STAGE.upper()
    MINIO_ACCESS_KEY = require_env(f"{prefix}_USER")
    MINIO_SECRET_KEY = require_env(f"{prefix}_PASS")

# =========================================================
# 🔹 BUCKET CONFIG
# =========================================================
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "temp")
MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
MINIO_SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "silver")
MINIO_GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "gold")

# =========================================================
# 🔹 SPARK CONFIG
# =========================================================
SPARK_APP_DEFAULT = "lakehouse_app"
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")

# =========================================================
# 🔹 DEBUG SNAPSHOT (optional but powerful)
# =========================================================
print("[CONFIG] MinIO Endpoint:", MINIO_ENDPOINT)
print("[CONFIG] Access Key (masked):", MINIO_ACCESS_KEY[:3] + "***")
