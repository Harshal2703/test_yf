import os
import time
import csv
import signal
import threading
from queue import Queue, Empty

import yfinance as yf


# ============================================================
# Configuration
# ============================================================

TICKERS = [
    # Major Indian Market Indices
    "^NSEI",          # NIFTY 50
    "^BSESN",         # BSE SENSEX
    "^NSEBANK",       # NIFTY BANK
    "^NIFTYJR",       # NIFTY NEXT 50 (verify availability)

    # Banking & Financial Services
    "^CNXFINANCE",    # NIFTY FINANCIAL SERVICES
    "^CNXPSUBANK",    # NIFTY PSU BANK

    # IT & Technology
    "^CNXIT",         # NIFTY IT

    # Automobile
    "^CNXAUTO",       # NIFTY AUTO

    # Healthcare & Pharmaceuticals
    "^CNXPHARMA",     # NIFTY PHARMA

    # Consumer & FMCG
    "^CNXFMCG",       # NIFTY FMCG

    # Metals & Mining
    "^CNXMETAL",      # NIFTY METAL

    # Energy & Infrastructure
    "^CNXENERGY",     # NIFTY ENERGY
    "^CNXINFRA",      # NIFTY INFRASTRUCTURE

    # Real Estate
    "^CNXREALTY",     # NIFTY REALTY

    # Media
    "^CNXMEDIA",      # NIFTY MEDIA

    # Other sectors
    "^CNXMNC",        # NIFTY MNC
    "^CNXPSE",        # NIFTY PSE
    "^CNXSERVICE",    # NIFTY SERVICES SECTOR
]

# Remove duplicate subscriptions
TICKERS = list(dict.fromkeys(TICKERS))

OUTPUT_DIR = "tick_data"

# Maximum number of ticks waiting in memory
QUEUE_MAX_SIZE = 100_000

# CSV filename
CSV_FILE = "ticks.csv"

QUEUE_TIMEOUT = 1.0


# ============================================================
# Global state
# ============================================================

STOP_EVENT = threading.Event()

TICK_QUEUE = Queue(maxsize=QUEUE_MAX_SIZE)


# ============================================================
# Utilities
# ============================================================

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_tick(msg):
    """
    Convert Yahoo WebSocket message into:

        (symbol, timestamp, price, volume)
    """

    try:
        symbol = str(msg.get("id", "")).upper()

        if not symbol:
            return None

        price = to_float(msg.get("price"))
        volume = to_float(msg.get("day_volume"))

        timestamp = msg.get("time")
        receive_timestamp = int(time.time() * 1000)

        return (
            symbol,
            timestamp,
            price,
            volume,
            receive_timestamp
        )

    except Exception as exc:
        print("build_tick error:", exc)
        return None


# ============================================================
# WebSocket callback
# ============================================================

def handler(msg):
    """
    Very lightweight callback.

    No CSV writing here.
    """

    if STOP_EVENT.is_set():
        return

    tick = build_tick(msg)

    if tick is None:
        return

    try:
        TICK_QUEUE.put(tick, timeout=1)

    except Exception:
        print("WARNING: Tick queue is full")


# ============================================================
# CSV Writer
# ============================================================

def csv_writer_worker():

    print("CSV writer started")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_path = os.path.join(
        OUTPUT_DIR,
        CSV_FILE,
    )

    file_exists = os.path.exists(file_path)

    try:

        with open(
            file_path,
            "a",
            newline="",
            buffering=1,
        ) as csv_file:

            writer = csv.writer(csv_file)

            # Write header only for a new file
            if not file_exists or os.path.getsize(file_path) == 0:

                writer.writerow([
                    "symbol",
                    "timestamp",
                    "price",
                    "volume",
                ])

                csv_file.flush()

            while True:

                # Stop only after queue has completely drained
                if STOP_EVENT.is_set() and TICK_QUEUE.empty():
                    break

                try:
                    tick = TICK_QUEUE.get(
                        timeout=QUEUE_TIMEOUT
                    )

                except Empty:
                    continue

                try:

                    symbol, timestamp, price, volume , receive_timestamp = tick

                    writer.writerow([
                        symbol,
                        timestamp,
                        price,
                        volume,
                        receive_timestamp
                    ])

                    # Make data visible on disk immediately
                    csv_file.flush()

                finally:
                    TICK_QUEUE.task_done()

    except Exception as exc:

        print(
            "CSV writer error:",
            exc,
        )

        STOP_EVENT.set()

    print("CSV writer stopped")


# ============================================================
# WebSocket
# ============================================================

def websocket_worker():

    print(
        f"Starting WebSocket "
        f"with {len(TICKERS)} unique tickers"
    )

    try:

        with yf.WebSocket() as ws:

            ws.subscribe(TICKERS)

            print(
                "WebSocket subscribed:",
                TICKERS,
            )

            ws.listen(handler)

    except Exception as exc:

        if not STOP_EVENT.is_set():

            print(
                "WebSocket error:",
                exc,
            )

            STOP_EVENT.set()


# ============================================================
# Shutdown
# ============================================================

def stop_handler(signum, frame):

    print(
        f"\nReceived signal {signum}"
    )

    STOP_EVENT.set()


# ============================================================
# Main
# ============================================================

def main():

    signal.signal(
        signal.SIGINT,
        stop_handler,
    )

    signal.signal(
        signal.SIGTERM,
        stop_handler,
    )

    # CSV writer
    writer_thread = threading.Thread(
        target=csv_writer_worker,
        name="CSVWriter",
    )

    # WebSocket
    websocket_thread = threading.Thread(
        target=websocket_worker,
        name="WebSocket",
    )

    writer_thread.start()
    websocket_thread.start()

    print("Collector started.")
    print("Press Ctrl+C to stop.")

    try:

        while not STOP_EVENT.is_set():
            time.sleep(1)

    except KeyboardInterrupt:

        STOP_EVENT.set()

    print("Stopping collector...")

    # --------------------------------------------------------
    # Stop WebSocket
    # --------------------------------------------------------

    websocket_thread.join(timeout=5)

    if websocket_thread.is_alive():

        print(
            "WARNING: WebSocket thread "
            "did not stop within timeout"
        )

    # --------------------------------------------------------
    # Wait for every queued tick to be written
    # --------------------------------------------------------

    print("Waiting for queued ticks...")

    TICK_QUEUE.join()

    # --------------------------------------------------------
    # Stop CSV writer
    # --------------------------------------------------------

    writer_thread.join(timeout=30)

    if writer_thread.is_alive():

        print(
            "WARNING: CSV writer "
            "did not stop within timeout"
        )

    print("Collector shutdown complete.")


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()
