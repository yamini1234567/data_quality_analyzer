# test_ollama.py
import requests
import json

print("Testing Ollama with Python...\n")

url = "http://localhost:11434/api/generate"

data = {
    "model": "llama3.2:1b",
    "prompt": "What is data quality? Answer in 2 sentences.",
    "stream": False
}

print(f"Question: {data['prompt']}\n")
print("Getting answer from AI...\n")

response = requests.post(url, json=data)

if response.status_code == 200:
    result = response.json()
    print("Answer:")
    print(result['response'])
    print("\n✅ Ollama is working with Python!")
else:
    print(f"❌ Error: {response.status_code}")