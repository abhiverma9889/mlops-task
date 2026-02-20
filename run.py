import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys
from pathlib import Path

def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def write_error(output, version, message):
    error_json = {
        "version": version,
        "status": "error",
        "error_message": message,
    }
    with open(output, "w") as f:
        json.dump(error_json, f, indent=2)

def main(args):
    start_time = time.time()
    version = "unknown"

    try:
        logging.info("Job started")

        if not Path(args.config).exists():
            raise FileNotFoundError("Config file not found")

        with open(args.config) as f:
            config = yaml.safe_load(f)

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(
            f"Config loaded: seed={seed}, window={window}, version={version}"
        )

        if not Path(args.input).exists():
            raise FileNotFoundError("Input CSV file missing")

        df = pd.read_csv(args.input)

        if df.empty:
            raise ValueError("Input CSV is empty")

        if "close" not in df.columns:
            raise ValueError("Required column 'close' missing")

        logging.info(f"Data loaded: {len(df)} rows")

        df["rolling_mean"] = (
            df["close"].rolling(window=window, min_periods=1).mean()
        )

        logging.info(f"Rolling mean calculated with window={window}")

        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        logging.info("Signals generated")

        rows_processed = len(df)
        signal_rate = df["signal"].mean()

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        logging.info(
            f"Metrics: signal_rate={metrics['value']}, rows_processed={rows_processed}"
        )
        logging.info(
            f"Job completed successfully in {latency_ms}ms"
        )

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        logging.error(str(e))
        write_error(args.output, version, str(e))
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    args = parser.parse_args()

    setup_logger(args.log_file)
    main(args)
