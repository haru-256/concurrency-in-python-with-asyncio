import functools
from collections import defaultdict


def map_frequency(text: str) -> dict[str, int]:
    words = text.split(" ")
    frequencies: dict[str, int] = defaultdict(int)
    for word in words:
        frequencies[word] += 1
    return frequencies


def merge_dictionaries(first: dict[str, int], second: dict[str, int]) -> dict[str, int]:
    merged = first
    for key in second.keys():
        if key in merged:
            merged[key] += second[key]
        else:
            merged[key] = second[key]
    return merged


if __name__ == "__main__":
    lines = [
        "I know what I know",
        "I know that I know",
        "I don't know much",
        "They don't know much",
    ]

    mapped_results = [map_frequency(line) for line in lines]
    for result in mapped_results:
        print(result)
    print(functools.reduce(merge_dictionaries, mapped_results))
