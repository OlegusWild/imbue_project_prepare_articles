from datetime import datetime as dt
from utils import get_jsonl_lines, collect_urls_stat, get_most_frequent, collect_specific_uuids, get_random_samples, collect_m_texts_by_uuids


CUR_CHUNK_REL_PATH = 'articles_imbue/webz_2008_01-2013_12'
# CUR_CHUNK_REL_PATH = 'test_dir'


def run(chunk_path_rel, N=10, M=5):
    """
    Conducts representational experiment:
    1. Gets top N most frequent urls
    2. Gets samples of M articles for these urls and stores them at
        repr_experiments/

            {chunk_path}/

                url_stat.json

                n_m/

                    n_most_freq_urls.json
                    n_most_freq_url_uuids.json
                    
                    {datetime}/
                        random_m_samples.json
                        url_i_raw.txt
                        url_i_clean.txt
    """
    
    # prepare for experiments with m, n and this chunk
    collect_urls_stat(chunk_path_rel)

    get_most_frequent(chunk_path_rel, N, M)

    collect_specific_uuids(chunk_path_rel, N, M)

    # run an experiment
    exp_datetime = dt.now().strftime("%Y-%m-%dT%H-%M-%S")

    get_random_samples(chunk_path_rel, exp_datetime, N, M)

    collect_m_texts_by_uuids(chunk_path_rel, exp_datetime, N, M)


run(CUR_CHUNK_REL_PATH, 5, 2)