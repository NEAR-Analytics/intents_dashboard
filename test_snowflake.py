import os
import sys
from typing import Optional

from dotenv import load_dotenv

try:
    # Reuse the app's logic as much as possible
    from flipside_handler import (
        get_snowflake_connection,
        _load_private_key_bytes_from_env,  # type: ignore
        _resolve_private_key_path,          # type: ignore
    )
except Exception as e:  # pragma: no cover
    print(f"Failed to import flipside_handler: {e}")
    sys.exit(1)


def _detect_key_source() -> str:
    """Describe which key source appears to be configured."""
    if (os.getenv("PRIVATE_KEY_PEM") or "").strip():
        return "PRIVATE_KEY_PEM (raw PEM in env)"
    if (os.getenv("SNOWFLAKE_PRIVATE_KEY_B64") or "").strip():
        return "SNOWFLAKE_PRIVATE_KEY_B64 (base64 PEM in env)"
    if (os.getenv("SNOWFLAKE_PRIVATE_KEY") or "").strip():
        return "SNOWFLAKE_PRIVATE_KEY (raw PEM in env)"
    path = _resolve_private_key_path()
    if path:
        return f"key file at: {path}"
    return "no key configured"


def _print_env_summary() -> None:
    print("Snowflake env summary:")
    print(f"  SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"  SNOWFLAKE_USER:    {os.getenv('SNOWFLAKE_USER')}")
    print(f"  WAREHOUSE:         {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    print(f"  DATABASE:          {os.getenv('SNOWFLAKE_DATABASE')}")
    print(f"  SCHEMA:            {os.getenv('SNOWFLAKE_SCHEMA')}")
    pwd_set = bool((os.getenv('SNOWFLAKE_PRIVATE_KEY_PWD') or '').strip())
    print(f"  Key password set:  {pwd_set}")
    print(f"  Key source:        {_detect_key_source()}")


def _validate_env_key() -> Optional[bytes]:
    """Try parsing the key from env and return the derived DER bytes, or None if not env-based."""
    try:
        key_bytes = _load_private_key_bytes_from_env()
    except Exception as e:
        print("Key parse check: FAILED")
        print(f"  {type(e).__name__}: {e}")
        return None
    if key_bytes is not None:
        print("Key parse check: OK (env key parsed)")
        print(f"  DER length: {len(key_bytes)} bytes")
    else:
        print("Key parse check: skipped (using key file path)")
    return key_bytes


def main() -> int:
    load_dotenv()
    _print_env_summary()
    _validate_env_key()

    print("\nConnecting to Snowflake...")
    try:
        conn = get_snowflake_connection()
    except Exception as e:
        print("Connection: FAILED")
        print(f"  {type(e).__name__}: {e}")
        return 1

    print("Connection: OK")

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CURRENT_VERSION() AS version, CURRENT_USER() AS user, CURRENT_ROLE() AS role, CURRENT_ACCOUNT() AS account, CURRENT_WAREHOUSE() AS warehouse"
            )
            row = cur.fetchone()
            if row:
                # Order matches select list
                print("Session info:")
                print(f"  version:   {row[0]}")
                print(f"  user:      {row[1]}")
                print(f"  role:      {row[2]}")
                print(f"  account:   {row[3]}")
                print(f"  warehouse: {row[4]}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())


