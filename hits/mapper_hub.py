#!/usr/bin/env python3
import sys

for line in sys.stdin:
    parts = line.strip().split()
    node = parts[0]
    neighbors = parts[1:-2]
    hub = parts[-2]
    auth = float(parts[-1])

    # структура
    print(f"{node}\tLINKS\t{' '.join(neighbors)}\t{auth}")

    # вклад auth в hub
    for v in neighbors:
        print(f"{node}\tHUB\t{auth}")

