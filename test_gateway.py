import asyncio
import os
from agentos.provider.gateway import ProviderGateway

async def main():
    print("Checking environment...")
    print(f"Default Model: {os.getenv('AGENTOS_PROVIDER_DEFAULT_MODEL')}")
    print(f"Gemini Key Present: {'Yes' if os.getenv('GEMINI_API_KEY') else 'No'}")
    
    # Initialize your gateway
    gateway = ProviderGateway()
    
    test_messages = [
        {"role": "user", "content": "Hello! If you can read this, reply with 'The gateway is alive!'"}
    ]
    
    print("\nSending test prompt to Gemini...")
    try:
        response = await gateway.get_completion(test_messages)
        print(f"\nResponse from Gemini: {response}")
    except Exception as e:
        print(f"\nGateway test failed with error: {e}")

if __name__ == "__main__":
    asyncio.run(main())