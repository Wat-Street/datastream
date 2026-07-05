"""CLI for minting API keys: ``python -m core.auth generate [label]``."""

import sys

from core.auth import generate_key

if __name__ == "__main__":
    # usage: python -m core.auth generate [label]
    if len(sys.argv) >= 2 and sys.argv[1] == "generate":
        raw_key, env_line = generate_key(
            sys.argv[2] if len(sys.argv) >= 3 else "default"
        )
        print(f"key (give to client): {raw_key}")
        print(f"env line (add to API_KEYS): {env_line}")
    else:
        print("usage: python -m core.auth generate [label]")
        sys.exit(1)
