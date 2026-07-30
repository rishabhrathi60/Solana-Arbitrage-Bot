import os

from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("JUPITER_API_KEY")

if api_key:
    print("✅ API key loaded successfully!")
    print(f"Starts with: {api_key[:8]}...")
else:
    print("❌ API key was NOT found.")