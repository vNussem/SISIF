import json


def getText(path):
    fo = open(path, "r")

    lines = fo.readlines()

    query = ""

    count = 0
    for line in lines:
        count += 1
        # print(f'line {count}: {line}')
        query += line

    fo.close()

    return query


def getJson(path):
    # print(getText(path))
    return json.loads(getText(path))


# getJson("python\data-sync\etl-objects\portas-chestionar-mtpl.json")
