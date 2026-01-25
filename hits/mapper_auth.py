#!/usr/bin/env python3
import sys

for line in sys.stdin:
    parts = line.strip().split()
    node = parts[0]
    neighbors = parts[1:-2]
    hub = float(parts[-2])
    auth = parts[-1]

    # Передаём структуру графа
    print(f"{node}\tLINKS\t{' '.join(neighbors)}\t{hub}")

    # Передаём вклад hub во входящих соседей
    for v in neighbors:
        print(f"{v}\tAUTH\t{hub}")

