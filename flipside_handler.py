import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
import base64
import re
import textwrap
import requests
import json
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

def _try_rest_api_with_token(sql_query: str) -> pd.DataFrame:
    """Try to execute query using Snowflake REST API with token"""
    SNOWFLAKE_TOKEN = _get_config_value('SNOWFLAKE_TOKEN')
    SNOWFLAKE_ACCOUNT = _get_config_value('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = _get_config_value('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = _get_config_value('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = _get_config_value('SNOWFLAKE_SCHEMA')
    
    if not all([SNOWFLAKE_TOKEN, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE]):
        raise ValueError("Missing required parameters for REST API")
    
    # Snowflake REST API endpoint - try different URL formats
    possible_urls = []
    
    if '.' in SNOWFLAKE_ACCOUNT:
        # Format like "zsniary.flipside_pro" 
        account_parts = SNOWFLAKE_ACCOUNT.split('.')
        possible_urls = [
            f"https://{account_parts[0]}-{account_parts[1]}.snowflakecomputing.com",  # zsniary-flipside_pro
            f"https://{account_parts[0]}.{account_parts[1]}.snowflakecomputing.com",   # zsniary.flipside_pro
            f"https://app.snowflake.com/{account_parts[0]}/{account_parts[1]}",        # app.snowflake.com format
        ]
    else:
        possible_urls = [f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"]
    
    base_url = possible_urls[0]  # Start with first option
    
    headers = {
        'Authorization': f'Bearer {SNOWFLAKE_TOKEN}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Try to execute query using REST API
    payload = {
        'statement': sql_query,
        'warehouse': SNOWFLAKE_WAREHOUSE,
        'database': SNOWFLAKE_DATABASE,
        'schema': SNOWFLAKE_SCHEMA
    }
    
    response = requests.post(
        f"{base_url}/api/v2/statements",
        headers=headers,
        json=payload,
        timeout=30
    )
    
    if response.status_code == 200:
        result = response.json()
        # Extract data from REST API response and convert to DataFrame
        if 'data' in result and result['data']:
            columns = [col['name'].lower() for col in result['resultSetMetaData']['rowType']]
            data = result['data']
            return pd.DataFrame(data, columns=columns)
        else:
            return pd.DataFrame()
    else:
        raise Exception(f"REST API failed: {response.status_code} - {response.text}")

def get_snowflake_connection():
    """Create and return a Snowflake connection using JWT authentication"""
    # Get required configuration
    SNOWFLAKE_USER = _get_config_value('SNOWFLAKE_USER')
    SNOWFLAKE_ACCOUNT = _get_config_value('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = _get_config_value('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = _get_config_value('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = _get_config_value('SNOWFLAKE_SCHEMA')
    SNOWFLAKE_AUTHENTICATOR = _get_config_value('SNOWFLAKE_AUTHENTICATOR')
    SNOWFLAKE_PRIVATE_KEY_B64 = _get_config_value('SNOWFLAKE_PRIVATE_KEY_B64')
    
    # Validate required vars
    required_vars = {
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
        'SNOWFLAKE_DATABASE': SNOWFLAKE_DATABASE,
    }
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Handle base64 encoded private key
    if SNOWFLAKE_PRIVATE_KEY_B64:
        try:
            # Clean up the base64 string (remove quotes and whitespace)
            b64_key = SNOWFLAKE_PRIVATE_KEY_B64.strip().strip('"')
            
            # Decode base64 to get PEM content
            pem_bytes = base64.b64decode(b64_key)
            
            # Parse the PEM content to get DER PKCS8 bytes
            from cryptography.hazmat.primitives import serialization
            private_key = serialization.load_pem_private_key(pem_bytes, password=None)
            der_pkcs8 = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            
            # Use the DER key bytes for connection
            base_params = {
                'account': SNOWFLAKE_ACCOUNT,
                'user': SNOWFLAKE_USER,
                'authenticator': SNOWFLAKE_AUTHENTICATOR or 'SNOWFLAKE_JWT',
                'private_key': der_pkcs8,
                'warehouse': SNOWFLAKE_WAREHOUSE,
                'database': SNOWFLAKE_DATABASE,
                'schema': SNOWFLAKE_SCHEMA
            }
            
            return snowflake.connector.connect(**base_params)
            
        except Exception as e:
            print(f"Base64 key authentication failed: {e}")
            
    # Try key contents from env variables
    key_bytes = _load_private_key_bytes_from_env()
    if key_bytes is not None:
        base_params = {
            'account': SNOWFLAKE_ACCOUNT,
            'user': SNOWFLAKE_USER,
            'authenticator': SNOWFLAKE_AUTHENTICATOR or 'SNOWFLAKE_JWT',
            'private_key': key_bytes,
            'warehouse': SNOWFLAKE_WAREHOUSE,
            'database': SNOWFLAKE_DATABASE,
            'schema': SNOWFLAKE_SCHEMA
        }
        return snowflake.connector.connect(**base_params)

    # Try key file path
    key_path = _resolve_private_key_path()
    if key_path:
        base_params = {
            'account': SNOWFLAKE_ACCOUNT,
            'user': SNOWFLAKE_USER,
            'authenticator': SNOWFLAKE_AUTHENTICATOR or 'SNOWFLAKE_JWT',
            'private_key_file': key_path,
            'warehouse': SNOWFLAKE_WAREHOUSE,
            'database': SNOWFLAKE_DATABASE,
            'schema': SNOWFLAKE_SCHEMA
        }
        return snowflake.connector.connect(**base_params)

    raise FileNotFoundError(
        "No authentication method available. Provide either:\n"
        "1. SNOWFLAKE_PRIVATE_KEY_B64 (base64 encoded private key), or\n"
        "2. SNOWFLAKE_PRIVATE_KEY_FILE (path to private key file), or\n"
        "3. PRIVATE_KEY_PEM (raw PEM content)"
    )

def get_fs_data(query_path, query_text=None, page_number=1, page_size=1):
    """
    Execute a SQL query using Snowflake connection or REST API
    
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
    
    # Use Python connector with RSA key authentication
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
