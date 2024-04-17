import json
import jsonlines
import pathlib
import os
import sys

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
# for path_obj in pathlib.Path('C:/Users/Oleg/Downloads/articles_imbue').rglob('*.json'):
#     with jsonlines.open(path_obj) as reader:

#         for record in reader:
            
#             # get record format
#             format = []
#             for field_name in record.keys():
#                 format.append(field_name)
            
#             formats.add(tuple(sorted(format)))

# assert(len(formats) == 1)

# sys.getsizeof(x)

# with open('C:/Users/Oleg/Desktop/test_output.jsonl') as reader:
#     print(os.fstat(reader.fileno()).st_size)

# keeping track of the progress
articles_processed = 0

# gather all related files
for path_obj in pathlib.Path('C:/Users/Oleg/Downloads/articles_imbue/webz_2008_01-2013_12').rglob('*.json'):

    # working with a particular chunk as with a list of python dictionaries
    with jsonlines.open(path_obj) as reader:

        for record in reader:
            
            # maybe as size grows just split to another to be able to open
            # os.fstat(a.fileno()).st_size
            with jsonlines.open('C:/Users/Oleg/Desktop/output1.jsonl', mode='a') as writer:
                writer.write({
                    'title': record['title'],
                    'url': record['url'],
                    'text': record['text'],
                    'date_published': record['published'],
                    'uuid': record['uuid']
                })
            
            articles_processed += 1

            if articles_processed % 10000 == 0:
                print(articles_processed)
