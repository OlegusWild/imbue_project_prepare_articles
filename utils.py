import json
import csv
from random import sample


def get_most_frequent(n=10):
    with open('stat.json') as f:
        stat_dict = json.load(f)

        with open('most_frequent_2008.json', 'w') as f:
            res = [(key, val) for key, val in stat_dict.items() if val >= 1000]
            res.sort(key=lambda x: x[1], reverse=True)
            res = res[:n]
            res = [i[0] for i in res]
            print(len(res))
            f.write(json.dumps(res))


def get_random_samples(m):
    with open('url_uuids_2008.json') as f:
        stat_dict = json.load(f)

        for url, uuid_arr in stat_dict.items():

            with open('random_samples_for_most_frequent_2008.json') as f:
                cur_dict = json.load(f)
                if m <= len(uuid_arr):
                    cur_dict[url] = sample(uuid_arr, m)

                with open('random_samples_for_most_frequent_2008.json', 'w') as f:
                    f.write(json.dumps(cur_dict))
