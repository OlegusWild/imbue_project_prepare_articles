import os
import json, jsonlines
import pathlib
from urllib.parse import urlparse
from articles_cleaner import get_cleaned_text
from random import sample
from difflib import Differ


ROOT_PATH = 'C:/Users/Oleg/imbue_project_prepare_articles'


def get_jsonl_lines(path):
    progress = 0

    for path_obj in pathlib.Path(path).rglob('*.json'):

        # working with a particular chunk as with a list of python dictionaries
        with jsonlines.open(path_obj) as reader:

            for record in reader:

                yield record, str(path_obj)
                progress += 1
                if progress % 1e5 == 0:
                    print(f'{progress} processed')


def collect_urls_stat(rel_path):
    """
    Collects and stores url frequency statistics
    """
    file_path = f'{ROOT_PATH}/repr_experiments/{str(rel_path)}/url_stat.json'

    # already computed
    if pathlib.Path(file_path).is_file():
        print(f'Dir {file_path} already exists!')
        return
    
    print(f'Creating {file_path}')
    
    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{str(rel_path)}').mkdir(parents=True, exist_ok=True)

    stat_dict = {}

    for record, _path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{rel_path}'):
        url_domain = extract_main_domain(record['url'])
        stat_dict[url_domain] = stat_dict.get(url_domain, 0) + 1
            
    with open(f'{file_path}', 'w') as f:
        f.write(json.dumps(stat_dict))


def get_most_frequent(rel_path, n, m):

    file_path = f'{ROOT_PATH}/repr_experiments/{rel_path}/{n}_{m}/{n}_most_frequent_urls.json'

    # already computed
    if pathlib.Path(file_path).is_file():
        print(f'Dir {file_path} already exists!')
        return
    
    print(f'Creating {file_path}')

    with open(f'repr_experiments/{rel_path}/url_stat.json') as f:
        stat_dict = json.load(f)

        pathlib.Path(f'{ROOT_PATH}/repr_experiments/{rel_path}/{n}_{m}').mkdir(parents=True, exist_ok=True)

        with open(f'repr_experiments/{rel_path}/{n}_{m}/{n}_most_frequent_urls.json', 'w') as f:
            res = [(key, val) for key, val in stat_dict.items()]
            res.sort(key=lambda x: x[1], reverse=True)
            res = res[:n]
            res = [i[0] for i in res]
            print(len(res))
            f.write(json.dumps(res))


def collect_specific_uuids(rel_path, n, m):
    file_path = f'{ROOT_PATH}/repr_experiments/{rel_path}/{n}_{m}/{n}_most_frequent_url_uuids.json'

    # already computed
    if pathlib.Path(file_path).is_file():
        print(f'Dir {file_path} already exists!')
        return
    
    print(f'Creating {file_path}')
    
    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{rel_path}/{n}_{m}').mkdir(parents=True, exist_ok=True)

    with open(f'repr_experiments/{rel_path}/{n}_{m}/{n}_most_frequent_urls.json') as f:
        target_urls = set(json.load(f))

    url_uuids = {}

    for record, _path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{rel_path}'):
        url_domain = extract_main_domain(record['url'])

        if url_domain in target_urls:
            if url_domain not in url_uuids:
                url_uuids[url_domain] = []
            url_uuids[url_domain].append(record['uuid'])
            
    with open(f'{file_path}', 'w') as f:
        f.write(json.dumps(url_uuids))


def collect_uuids(rel_path):
    file_path = f'{ROOT_PATH}/repr_experiments/{rel_path}/uuids.json'

    # already computed
    if pathlib.Path(file_path).is_file():
        print(f'Dir {file_path} already exists!')
        return
    
    print(f'Creating {file_path}')
    
    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{rel_path}').mkdir(parents=True, exist_ok=True)

    uuids = []
    for record, _path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{rel_path}'):
        uuids.append(record['uuid'])
    
    with open(file_path, 'w') as f:
        f.write(json.dumps(uuids))


def get_random_samples(path_rel, dt_str, n, m):
    
    file_path = f'{ROOT_PATH}/repr_experiments/{path_rel}/{n}_{m}/{dt_str}/{m}_uuids.json'

    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{path_rel}/{n}_{m}/{dt_str}').mkdir(parents=True, exist_ok=True)

    # creates a file
    with open(file_path, 'w') as f:
        f.write(json.dumps([]))


    with open(f'repr_experiments/{path_rel}/{n}_{m}/{n}_most_frequent_url_uuids.json') as f:
        stat_dict = json.load(f)

        result_uuids = []
        for url, uuid_arr in stat_dict.items():

            if m > len(uuid_arr):
                m = len(uuid_arr)
            result_uuids.extend(sample(uuid_arr, m))

    with open(file_path, 'w') as f:
        f.write(json.dumps(result_uuids))


def get_random_samples_from_all(path_rel, m, dt_str):
    with open(f'repr_experiments/{path_rel}/uuids.json') as f:
        uuids = json.load(f)
        if m > len(uuids):
            m = len(uuids)
        m_uuids_sample = sample(uuids, m)

    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{path_rel}/{m}/{dt_str}').mkdir(parents=True, exist_ok=True)
    
    with open(f'repr_experiments/{path_rel}/{m}/{dt_str}/{m}_uuids.json', 'w') as f:
        f.write(json.dumps(m_uuids_sample))


def collect_m_texts_by_uuids(path_rel, dt_str, n, m):
    
    with open(f'repr_experiments/{path_rel}/{n}_{m}/{dt_str}/{m}_uuids.json') as f:
        target_uuids = set(json.load(f))
    
    url_sample_num = {}

    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{path_rel}/{n}_{m}/{dt_str}/results').mkdir(parents=True, exist_ok=True)

    for record, _path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{path_rel}'):
        
        if record['uuid'] in target_uuids:
        
            url_domain = extract_main_domain(record['url'])
            if url_domain not in url_sample_num:
                url_sample_num[url_domain] = 1
            else:
                url_sample_num[url_domain] += 1

            with open(f'repr_experiments/{path_rel}/{n}_{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_raw.txt', 'w', encoding='utf-8') as f:
                f.write(record['text'])
            
            # applying a cleaning technique
            with open(f'repr_experiments/{path_rel}/{n}_{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_clean.txt', 'w', encoding='utf-8') as f:
                clean_text = get_cleaned_text(record['text'])
                f.write(clean_text)
            
            # getting the difference
            with open(f'repr_experiments/{path_rel}/{n}_{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_diff.txt', 'w', encoding='utf-8') as f:
                f.writelines(compare_texts(record['text'], clean_text))


def collect_m_texts_by_uuids_from_all(path_rel, dt_str, m):
    
    with open(f'repr_experiments/{path_rel}/{m}/{dt_str}/{m}_uuids.json') as f:
        target_uuids = set(json.load(f))
    
    url_sample_num = {}

    pathlib.Path(f'{ROOT_PATH}/repr_experiments/{path_rel}/{m}/{dt_str}/results').mkdir(parents=True, exist_ok=True)

    for record, _path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{path_rel}'):
        
        if record['uuid'] in target_uuids:
        
            url_domain = extract_main_domain(record['url'])
            if url_domain not in url_sample_num:
                url_sample_num[url_domain] = 1
            else:
                url_sample_num[url_domain] += 1

            with open(f'repr_experiments/{path_rel}/{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_raw.txt', 'w', encoding='utf-8') as f:
                f.write(record['text'])
            
            # applying a cleaning technique
            with open(f'repr_experiments/{path_rel}/{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_clean.txt', 'w', encoding='utf-8') as f:
                clean_text = get_cleaned_text(record['text'])
                f.write(clean_text)
            
            # getting the difference
            with open(f'repr_experiments/{path_rel}/{m}/{dt_str}/results/{url_domain}_{url_sample_num[url_domain]}_diff.txt', 'w', encoding='utf-8') as f:
                f.writelines(compare_texts(record['text'], clean_text))


def compare_texts(text1, text2):
    d = Differ()
    return list(d.compare(text1.splitlines(1), text2.splitlines(1)))


def extract_main_domain(url_str: str):
    """
    urls = [
        "https://mobile.reuters.ua",
        "https://news.reuters.uk",
        "https://money.reuters.com"
    ]
    have the same main domain name reuters
    """
    parsed_url = urlparse(url_str)
    domain_parts = parsed_url.netloc.split('.')
    if len(domain_parts) >= 2:
        return domain_parts[-2]  
    else:
        return domain_parts[0]


# in MB
RESULT_CHUNK_SIZE_BYTES = 500*1e6

file_num = 1


def clean_and_store(chunk_rel_path):
    """
    Stores extracted and cleaned data in chunks of specified format
    """
    global file_num

    pathlib.Path(f'{ROOT_PATH}/cleaned_data/{chunk_rel_path}').mkdir(parents=True, exist_ok=True)

    for record, path_str in get_jsonl_lines(f'{ROOT_PATH}/data/{chunk_rel_path}'):

        with jsonlines.open(f'{ROOT_PATH}/cleaned_data/{chunk_rel_path}/output_{file_num}.jsonl', mode='a') as writer:

            writer.write({
                'title': record['title'],
                'url': record['url'],
                'text': get_cleaned_text(record['text']),
                'date_published': record['published'],
                'uuid': record['uuid'],
                'file_path': path_str
            })

        if os.path.getsize(f'{ROOT_PATH}/cleaned_data/{chunk_rel_path}/output_{file_num}.jsonl') > RESULT_CHUNK_SIZE_BYTES:
            file_num += 1

