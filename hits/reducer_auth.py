#!/usr/bin/env python3
import sys
import math

current = None
auth_sum = 0.0
hub = 0.0
links = []
nodes = []

def emit(node, links, hub, auth):
    nodes.append((node, links, hub, auth))

for line in sys.stdin:
    parts = line.strip().split('\t')
    node = parts[0]

    if current and node != current:
        emit(current, links, hub, auth_sum)
        auth_sum = 0.0
        links = []
        hub = 0.0

    current = node

    if parts[1] == "LINKS":
        links = parts[2].split() if len(parts) > 2 else []
        hub = float(parts[3])
    else:  # AUTH
        auth_sum += float(parts[2])

if current:
    emit(current, links, hub, auth_sum)

norm = math.sqrt(sum(a*a for _,_,_,a in nodes))

for node, links, hub, auth in nodes:
    auth = auth / norm if norm != 0 else 0
    print(f"{node} {' '.join(links)} {hub} {auth}")
