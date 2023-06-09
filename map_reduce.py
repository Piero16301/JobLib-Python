from multiprocessing import Pool
import csv
import time


def readFile(filename):
    data = []
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row[0] == 'CA':
                data.append(row)
    return data[1:]


# mapper function
def count_words(names):
    count_names = {}
    for name in names:
        if not name in count_names:
            count_names[name] = 0
        count_names[name] += 1
    # print(count_names)
    return count_names


# my mapper function
def count_names(line):
    name = line[3]
    if line[0] != 'CA':
        return {name: 0}
    # print(count_names)
    return {name: int(line[4])}


# reduce function
# dict1: es el resultado previo que se va acumulando
# dict2: es el nuevo diccionario
def calculate(dict1, dict2):
    combined = {}

    # print(dict1, dict2)

    for key in dict1:
        combined[key] = dict1[key]

    for key in dict2:
        if key in combined:
            combined[key] += dict2[key]
        else:
            combined[key] = dict2[key]

    return combined


def reduce(results):
    total = {}
    for result in results:
        if list(result.keys())[0] in total:
            total[list(result.keys())[0]] += list(result.values())[0]
        else:
            total[list(result.keys())[0]] = list(result.values())[0]
    print('Reduce done:')
    return total


if __name__ == "__main__":
    filename = 'baby-names-state.csv'
    rowsFile = readFile(filename)

    start_time = time.time()
    with Pool(4) as pool:
        # results = pool.map(count_words, list_of_names) # MAP
        results = pool.map(count_names, rowsFile)  # MAP
        print('Map done:')
        # print(results)
        end_time = time.time()
        execution_time = end_time - start_time
        print("Execution time:", execution_time, "seconds")

    total = reduce(results)

    max_value = max(total.values())
    max_key = max(total, key=lambda k: total[k])
    print("The most used name in California is:", max_key, "with", max_value, "uses")
    # for key, val in words.items():
    #     print("El total para {} es {}".format(key, val))
