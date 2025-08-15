import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
import base64
import re
import textwrap
load_dotenv()

# Snowflake connection parameters - JWT authentication
def _get_config_value(key: str) -> str | None:
    """Fetch configuration from environment or Streamlit secrets if available."""
    val = os.getenv(key)
    if val is not None and str(val).strip() != '':
        return str(val)
    try:
        import streamlit as st  # local import to avoid hard dependency at import time
        secret_val = st.secrets.get(key)
        if secret_val is not None and str(secret_val).strip() != '':
            return str(secret_val)
    except Exception:
        pass
    return None

def _resolve_private_key_path() -> str | None:
    """Resolve the private key file path from env or common defaults."""
    base_dir = os.path.dirname(__file__)

    # If env var provided, try absolute then relative to project
    env_key_file = _get_config_value('SNOWFLAKE_PRIVATE_KEY_FILE')
    if env_key_file:
        candidate = env_key_file
        if os.path.isabs(candidate) and os.path.exists(candidate):
            return candidate
        rel_candidate = os.path.join(base_dir, candidate)
        if os.path.exists(rel_candidate):
            return rel_candidate

    # Fallbacks commonly used in this repo
    for fname in [
        'rsa_key.p8',            # password-protected typical name
        'rsa_key_nopass.p8',     # unprotected key present in repo
    ]:
        candidate = os.path.join(base_dir, fname)
        if os.path.exists(candidate):
            return candidate

    return None

def _load_private_key_bytes_from_env() -> bytes | None:
    """Attempt to load a private key from env vars and return DER PKCS8 bytes.

    Preferred: raw PEM in PRIVATE_KEY_PEM (can include real newlines or \n escapes,
    optionally surrounded by triple quotes). Also supports legacy raw PEM in
    SNOWFLAKE_PRIVATE_KEY and base64-encoded PEM in SNOWFLAKE_PRIVATE_KEY_B64.
    If none are present, returns None.
    """
    pem_bytes: bytes | None = None

    # 1) Preferred: PRIVATE_KEY_PEM (raw, possibly multi-line)
    private_key_pem = _get_config_value('PRIVATE_KEY_PEM')
    if private_key_pem and private_key_pem.strip():
        text = private_key_pem
        # Dedent to remove common indentation from .env formatting
        text = textwrap.dedent(text)
        # Normalize typical \n-escaped content if present
        if "\\n" in text and "\n" not in text:
            text = text.replace("\\n", "\n")
        # Strip outer whitespace
        text = text.strip()
        # Drop bare triple-quote lines that may remain from .env wrapping
        lines = [ln for ln in text.splitlines() if ln.strip() not in {'"""', "'''"}]
        # Keep only content between BEGIN and END markers if present
        begin_idx = next((i for i, ln in enumerate(lines) if '-----BEGIN ' in ln), None)
        end_idx = next((i for i, ln in enumerate(lines) if '-----END ' in ln), None)
        if begin_idx is not None and end_idx is not None and end_idx >= begin_idx:
            lines = lines[begin_idx:end_idx + 1]
        # Trim whitespace on each line
        lines = [ln.strip() for ln in lines if ln.strip()]
        text = "\n".join(lines) + "\n"
        pem_bytes = text.encode()

    # 2) Legacy: explicit base64 if provided
    private_key_b64 = _get_config_value('SNOWFLAKE_PRIVATE_KEY_B64')
    if pem_bytes is None and private_key_b64 and private_key_b64.strip():
        try:
            pem_bytes = base64.b64decode(private_key_b64.strip())
        except Exception:
            raise ValueError("Failed to base64-decode SNOWFLAKE_PRIVATE_KEY_B64")

    # 3) Legacy: raw PEM content
    raw_private_key = _get_config_value('SNOWFLAKE_PRIVATE_KEY')
    if pem_bytes is None and raw_private_key and raw_private_key.strip():
        text = raw_private_key.strip()
        # Allow \n-escaped values commonly used in env files
        if "\\n" in text and "\n" not in text:
            text = text.replace("\\n", "\n")
        pem_bytes = text.encode()

    if pem_bytes is None:
        return None

    # Convert PEM to DER PKCS8 bytes using cryptography
    from cryptography.hazmat.primitives import serialization
    # Detect encryption requirement from header
    header_match = re.search(br"-----BEGIN (ENCRYPTED )?PRIVATE KEY-----", pem_bytes)
    is_encrypted_header = bool(header_match and header_match.group(1))
    private_key_pwd = _get_config_value('SNOWFLAKE_PRIVATE_KEY_PWD')
    password_bytes = private_key_pwd.encode() if private_key_pwd else None
    if is_encrypted_header and password_bytes is None:
        raise ValueError("Encrypted private key detected in PRIVATE_KEY_PEM but SNOWFLAKE_PRIVATE_KEY_PWD is not set.")
    try:
        private_key = serialization.load_pem_private_key(
            pem_bytes,
            password=password_bytes,
        )
    except TypeError as e:
        # If password provided but key is not encrypted, retry without password
        if 'not encrypted' in str(e).lower() or 'password was given' in str(e).lower():
            private_key = serialization.load_pem_private_key(
                pem_bytes,
                password=None,
            )
        else:
            raise ValueError(f"Failed to parse private key from env: {e}")
    except Exception as e:
        raise ValueError(f"Failed to parse private key from env: {e}")

    der_pkcs8 = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return der_pkcs8

def get_snowflake_connection():
    """Create and return a Snowflake connection using JWT authentication"""
    # Validate required env vars (excluding key file which we resolve below)
    SNOWFLAKE_USER = _get_config_value('SNOWFLAKE_USER')
    SNOWFLAKE_ACCOUNT = _get_config_value('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = _get_config_value('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = _get_config_value('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = _get_config_value('SNOWFLAKE_SCHEMA')

    required_vars = {
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
        'SNOWFLAKE_DATABASE': SNOWFLAKE_DATABASE,
    }
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # First, prefer key contents from env if provided
    key_bytes = _load_private_key_bytes_from_env()
    if key_bytes is not None:
        base_params = {
            'account': SNOWFLAKE_ACCOUNT,
            'user': SNOWFLAKE_USER,
            'authenticator': 'SNOWFLAKE_JWT',
            'private_key': key_bytes,
            'warehouse': SNOWFLAKE_WAREHOUSE,
            'database': SNOWFLAKE_DATABASE,
            'schema': SNOWFLAKE_SCHEMA
        }
        return snowflake.connector.connect(**base_params)

    # Otherwise fall back to resolving a key file path
    key_path = _resolve_private_key_path()
    if not key_path:
        raise FileNotFoundError(
            "Snowflake private key not provided. Set PRIVATE_KEY_PEM with raw PEM (preferred), "
            "or SNOWFLAKE_PRIVATE_KEY / SNOWFLAKE_PRIVATE_KEY_B64, or set "
            "SNOWFLAKE_PRIVATE_KEY_FILE to a valid path, or place 'rsa_key.p8' or "
            "'rsa_key_nopass.p8' in the project directory."
        )

    # Build connection parameters for JWT authentication
    base_params = {
        'account': SNOWFLAKE_ACCOUNT,
        'user': SNOWFLAKE_USER,
        'authenticator': 'SNOWFLAKE_JWT',
        'private_key_file': key_path,
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA
    }

    # If password provided, try with it first; on TypeError for unencrypted key, retry without
    SNOWFLAKE_PRIVATE_KEY_PWD = _get_config_value('SNOWFLAKE_PRIVATE_KEY_PWD')
    if SNOWFLAKE_PRIVATE_KEY_PWD:
        try:
            return snowflake.connector.connect(
                **{**base_params, 'private_key_file_pwd': SNOWFLAKE_PRIVATE_KEY_PWD}
            )
        except TypeError as e:
            msg = str(e).lower()
            if 'not encrypted' in msg or 'unencrypted' in msg:
                # Retry without password
                return snowflake.connector.connect(**base_params)
            raise

    return snowflake.connector.connect(**base_params)

def get_fs_data(query_path, query_text=None, page_number=1, page_size=1):
    """
    Execute a SQL query using Snowflake connection instead of Flipside API
    
    Args:
        query_path: Path to SQL file containing the query
        query_text: Optional direct SQL query text (overrides query_path)
        page_number: Not used with Snowflake (kept for compatibility)
        page_size: Not used with Snowflake (kept for compatibility)
    
    Returns:
        pandas.DataFrame: Query results
    """
    # Get the query text
    if query_text is None:
        with open(query_path, 'r') as f:
            query_text = f.read()
    
    # Connect to Snowflake and execute query
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    try:
        # Execute the query
        cur.execute(query_text)
        
        # Fetch column names and convert to lowercase to match ClickHouse schema
        column_names = [desc[0].lower() for desc in cur.description]
        
        # Fetch all data and convert to DataFrame
        df = pd.DataFrame(cur.fetchall(), columns=column_names)
        
        # Convert datetime columns for ClickHouse compatibility
        for col in df.columns:
            if df[col].dtype.name.startswith('datetime'):
                if 'date' in col.lower() and 'timestamp' not in col.lower():
                    # Convert date columns to date format for ClickHouse Date type
                    df[col] = pd.to_datetime(df[col]).dt.date
                else:
                    # Convert timestamp columns to string format for ClickHouse String type
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        return df
        
    finally:
        cur.close()
        conn.close()
