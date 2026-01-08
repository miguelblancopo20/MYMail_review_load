from __future__ import annotations

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Workflow semanal (configurable via .config)")
    parser.add_argument("--fecha", default="4enero", help="Subcarpeta dentro de data/, por ejemplo: 4enero")
    return parser.parse_args()
