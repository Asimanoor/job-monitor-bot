#!/usr/bin/env python3
"""
Encode Google Service Account credentials to base64.
Usage:
    python encode_credentials.py
    python encode_credentials.py path/to/my_key.json

Copies the base64 string to clipboard if pyperclip is available.
"""

import base64
import os
import sys


def main() -> None:
    # Determine input file
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "credentials.json")

    if not os.path.isfile(path):
        print(f"❌ File not found: {path}")
        print("   Place your service-account JSON key as 'credentials.json' in this directory,")
        print("   or pass the path as an argument:")
        print("     python encode_credentials.py path/to/key.json")
        sys.exit(1)

    with open(path, "rb") as f:
        raw = f.read()

    encoded = base64.b64encode(raw).decode("ascii")

    print()
    print("=" * 70)
    print("  ✅ Base64-encoded credentials")
    print("=" * 70)
    print()
    print(encoded)
    print()
    print("=" * 70)
    print()
    print("📋 Copy the output above and add it as a GitHub Secret:")
    print("   Repository → Settings → Secrets → Actions → New repository secret")
    print("   Name:  GOOGLE_CREDENTIALS_JSON")
    print("   Value: <paste the base64 string>")
    print()
    print("⚠️  SECURITY WARNING:")
    print("   • NEVER commit credentials.json to git.")
    print("   • Add it to .gitignore (already done if you ran this repo setup).")
    print("   • If compromised, revoke the key in Google Cloud Console immediately.")
    print()

    # Try to copy to clipboard
    try:
        import pyperclip
        pyperclip.copy(encoded)
        print("📎 Copied to clipboard!")
    except ImportError:
        pass


if __name__ == "__main__":
    main()
