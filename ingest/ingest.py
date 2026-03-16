"""
🐋 Whale Watch - Etherscan Ingestion
Pulls recent large ETH transactions and stores in DuckDB.
"""
import os
import requests
import duckdb
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "whale_watch.duckdb")
WHALE_THRESHOLD_ETH = 100  # Minimum ETH to count as a whale transaction

def get_latest_block():
       """Get the latest Ethereum block number from Etherscan."""
       url =f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}"
       response = requests.get(url)
       data = response.json()
       # The result comes back as hex (e.g., "0x13a4b5c")
       # Convert to int: int(hex_string, 16)
       return int(data["result"], 16)
       
def get_block_transactions(block_number, retries=3):
       """Get all transactions in a given block."""
       block_hex = hex(block_number)
       url=f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_getBlockByNumber&tag={block_hex}&boolean=true&apikey={ETHERSCAN_API_KEY}"
       import time
       for attempt in range(retries):
           response = requests.get(url)
           data = response.json()
           result = data.get("result")
           if isinstance(result, dict) and "transactions" in result:
               return result["transactions"]
           # API returned an error or rate limit — wait and retry
           print(f"    Retrying block {block_number} ({attempt + 1}/{retries})...")
           time.sleep(2)
       # All retries exhausted — return empty list instead of crashing
       print(f"    Skipping block {block_number} after {retries} retries")
       return []
       
def filter_whales(transactions):
       """Filter transactions >= WHALE_THRESHOLD_ETH."""
       whales = []
       for tx in transactions:
           value_eth = int(tx["value"], 16) / 10**18
           if value_eth >= WHALE_THRESHOLD_ETH:
               whales.append({
                   "tx_hash": tx["hash"],
                   "from_address": tx["from"],
                   "to_address": tx.get("to", "contract_creation"),
                   "value_eth": value_eth,
                   "block_number": int(tx["blockNumber"], 16),
               })
       return whales
       
       
def save_to_duckdb(whales):
       os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
       con = duckdb.connect(DUCKDB_PATH)
       con.execute("""
           CREATE TABLE IF NOT EXISTS raw_whale_transactions (
               tx_hash VARCHAR PRIMARY KEY,
               from_address VARCHAR,
               to_address VARCHAR,
               value_eth DOUBLE,
               block_number BIGINT,
               ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
           )
       """)
       for w in whales:
           con.execute("""
               INSERT OR IGNORE INTO raw_whale_transactions
               (tx_hash, from_address, to_address, value_eth, block_number)
               VALUES (?, ?, ?, ?, ?)
           """, [w["tx_hash"], w["from_address"], w["to_address"], w["value_eth"],
 w["block_number"]])
       con.close()
       print(f"💾 Saved {len(whales)} whales to DuckDB")

def main():
       print("🐋 Whale Watch — Scanning for whale transactions...")

       # Step 1: Get latest block
       latest_block = get_latest_block()
       print(f"Latest block: {latest_block}")

       # Step 2: Scan last 5 blocks for whales
       all_whales = []
       for i in range(5):
           block_num = latest_block - i
           print(f"  Scanning block {block_num}...")
           transactions = get_block_transactions(block_num )
           if transactions:
               whales = filter_whales(transactions)
               all_whales.extend(whales)
               print(f"Found {len(transactions)} txs, {len(whales)} whales")

       # Step 3: Print results
       print(f"\n🐋 Total whale transactions found: {len(all_whales)}")
       for w in all_whales:
           print(f"{w['value_eth']:.2f} ETH | {w['from_address'][:10]}... →{w['to_address'][:10]}...")
       save_to_duckdb(all_whales)

if __name__ == "__main__":
       main()
