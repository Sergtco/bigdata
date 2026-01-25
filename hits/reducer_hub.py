#!/usr/bin/env python3
import sys
import math

current = None
hub_sum = 0.0
auth = 0.0
links = []
nodes = []

def emit(node, links, hub, auth):
    nodes.append((node, links, hub, auth))

for line in sys.stdin:
    parts = line.strip().split('\t')
    node = parts[0]

    if current and node != current:
        emit(current, links, hub_sum, auth)
        hub_sum = 0.0
        links = []
        auth = 0.0

    current = node

    if parts[1] == "LINKS":
        links = parts[2].split() if len(parts) > 2 else []
        auth = float(parts[3])
    else:  # HUB
        hub_sum += float(parts[2])

if current:
    emit(current, links, hub_sum, auth)

norm = math.sqrt(sum(h*h for _,_,h,_ in nodes))

for node, links, hub, auth in nodes:
    hub = hub / norm if norm != 0 else 0
    print(f"{node} {' '.join(links)} {hub} {auth}")
