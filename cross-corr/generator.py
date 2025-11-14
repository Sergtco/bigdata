import random

def generate_order_db(filename="order_db.txt"):
    with open(filename, "w") as f:
        for i in range(50000):
            line = f"{i}|"
            item_count = random.randint(1, 15)
            seen = set()

            for _ in range(item_count):
                item = random.randint(0, 999)
                while item in seen:
                    item = random.randint(0, 999)
                seen.add(item)
                line += f"item{item}|"

            line = line.rstrip("|") + "\n"
            f.write(line)


def check_duplicates_in_file(filename):
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split("|")
            items = parts[1:]
            if len(items) != len(set(items)):
                print("Duplicate found in:", line)
                return
    print("No duplicates found.")


if __name__ == "__main__":
    generate_order_db()
    check_duplicates_in_file("order_db.txt")
