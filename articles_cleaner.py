import jsonlines, json
import pathlib
import os
from urllib.parse import urlparse
import csv

# High-level plan:
# 1. download files
# 2. unpack files
# 3. try to get all records into big one jsonl persisting the unified format same for all records:
#   title
#   url
#   text
#   date (published I assume)
#   ideally also a pointer to the original record of some kind - can use uuid + name, should guarantee uniqueness
#   additionally meta dict (potentially useful fields)
# 4. creating a huge file VS. multiple smaller chunks of data of 100MB? Otherwise it's not openable by editor and hard to share

# make sure all records have the same format
# formats = set()
# for path_obj in pathlib.Path('C:/Users/Oleg/imbue_project_prepare_articles/data/test_dir').rglob('*.json'):
#     with jsonlines.open(path_obj) as reader:

#         for record in reader:
            
#             # get record format
#             format = []
#             for field_name in record.keys():
#                 format.append(field_name)
            
#             formats.add(tuple(sorted(format)))

# assert(len(formats) == 1)

# sys.getsizeof(x)

# # here's 25GB
MAIN_DIR = 'C:/Users/Oleg/imbue_project_prepare_articles/data/articles_imbue'
# in MB
RESULT_CHUNK_SIZE_BYTES = 5*1e6

# keeping track of the progress
articles_processed = 0

file_num = 1


def write_cleaned_rec(record: dict):
    """
    Stores extracted data in chunks of specified format
    """

    global articles_processed, file_num

    with jsonlines.open(f'C:/Users/Oleg/Desktop/test_output_{file_num}.jsonl', mode='a') as writer:

        writer.write({
            'title': record['title'],
            'url': record['url'],
            'text': record['text'],
            'date_published': record['published'],
            'uuid': record['uuid']
        })

    if os.path.getsize(f'C:/Users/Oleg/Desktop/test_output_{file_num}.jsonl') > RESULT_CHUNK_SIZE_BYTES:
        file_num += 1

    if articles_processed % 1e5 == 0:
        print(articles_processed)


def collect_urls_stat(record):
    """
    Collects and stores url frequency statistics
    """

    global articles_processed, file_num

    url_domain = urlparse(record['url']).netloc

    if url_domain:
        with open(f'urls_{file_num}.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([url_domain])
        
        articles_processed += 1

        if articles_processed % 1e5 == 0:
            with open('stat.json') as f:
                stat_dict = json.load(f)

                with open(f'urls_{file_num}.csv', newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        stat_dict[row[0]] = stat_dict.get(row[0], 0) + 1
            
            with open('stat.json', 'w') as f:
                f.write(json.dumps(stat_dict))

            file_num += 1
    
    else:
        with open('no_url_articles.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow([record['uuid']])


def collect_uuids_for_urls(record, target_urls: set):
    """
    output:
    {
        url1: [*uuids1],
        url2: [*uuids2]
    }
    """

    url_domain = urlparse(record['url']).netloc

    if url_domain in target_urls:
        
        with open('url_uuids_2008.json') as f:
            stat_dict = json.load(f)

            stat_dict[url_domain] = stat_dict.get(url_domain, [])
            stat_dict[url_domain].append(record['uuid'])
        
            with open('url_uuids_2008.json', 'w') as f:
                f.write(json.dumps(stat_dict))


def main():
    """
    Main loop of logic
    """

    # with open('most_frequent_2008.json') as f:
    #     target_urls = set(json.load(f))
    
    with open('random_samples_for_most_frequent_2008.json') as f:
        samples_dict = json.load(f)
        target_uuids = set(samples_dict["www.aljazeera.com"])

    # gather all related files
    for path_obj in pathlib.Path(MAIN_DIR + '/webz_2008_01-2013_12').rglob('*.json'):

        # working with a particular chunk as with a list of python dictionaries
        with jsonlines.open(path_obj) as reader:

            for record in reader:
                
                # collect_uuids_for_urls(record, target_urls)
                # write_cleaned_rec(record)
                # collect_urls_stat(record)
                if record['uuid'] in target_uuids:
                    with open('check_text_aljazeera.txt', 'a') as f:
                        f.write(record['text'])
            
  
main()