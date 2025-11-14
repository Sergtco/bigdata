import random

for i in range(50000):
    items = set(["item"+str(random.randint(0, 1000)) for _ in range(1, random.randint(1, 15))])
