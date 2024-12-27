import pathlib
import time
from collections import defaultdict


def main() -> None:
    file = pathlib.Path("./data/googlebooks-eng-all-1gram-20120701-a")
    freqs: dict[str, int] = defaultdict(int)

    with open(file, encoding="utf-8") as f:
        lines = f.readlines()

        start = time.perf_counter()

        for line in lines:
            data = line.split("\t")
            word = data[0]
            count = int(data[2])
            freqs[word] += count

        end = time.perf_counter()
        print(f"{end-start:.4f}")


if __name__ == "__main__":
    main()
