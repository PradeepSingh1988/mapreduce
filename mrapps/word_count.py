import argparse
from collections import defaultdict


def mapper(filename, contents):
    def remove_unwanted_chars(word):
        first_letter_exclusion = ['"', "'", "]", "[", "(", ")"]
        last_letter_exclusion = ["?", ".", "-", "!", ",", '"', "'", "]", "[", "(", ")"]
        if word[0] in first_letter_exclusion:
            word = word[1:]
        if word:
            if word[-1] in last_letter_exclusion:
                word = word[:-1]
        return word

    word_count_map_list = []
    for line in contents.split("\n"):
        line = line.strip()
        for word in [word for word in line.split() if line]:
            try:
                word = remove_unwanted_chars(word)
                word_count_map_list.append({word: 1})
            except Exception as ex:
                continue
                
    return word_count_map_list


def reducer(key, values):
    return len(values)


def main():
    parser = argparse.ArgumentParser(
        prog="Word Count",
        description="Execute the MapR jobs",
    )
    parser.add_argument("--input-file", required=True, help="Input File")

    parser.add_argument("--output-file", required=True, help="out put file")

    args = parser.parse_args()

    content = open(args.input_file).read()
    word_count_map_list = mapper(args.input_file, content)
    sorted_word_count_list = sorted(
        word_count_map_list, key=lambda x: list(x.keys())[0]
    )
    dict_list = defaultdict(list)
    for item in sorted_word_count_list:
        key, value = list(item.keys())[0], list(item.values())[0]
        dict_list[key].append(value)
    with open(args.output_file, "a") as f:
        for key, value in dict_list.items():
            output = reducer(key, value)
            f.write("{} {}\n".format(key, output))


if __name__ == "__main__":
    main()
