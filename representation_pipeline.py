from datetime import datetime as dt
from utils import collect_urls_stat, get_most_frequent, collect_specific_uuids, get_random_samples, collect_m_texts_by_uuids

from utils import collect_uuids, get_random_samples_from_all, collect_m_texts_by_uuids_from_all

CUR_CHUNK_REL_PATH = 'articles_imbue/webz_2008_01-2013_12'
# CUR_CHUNK_REL_PATH = 'test_dir'


def run(chunk_path_rel, N=10, M=10):
    """
    Conducts representational experiment

    Option 1:
    1. Gets top N most frequent urls
    2. Gets samples of M articles for these urls and stores them at
        repr_experiments/

            {chunk_path}/

                url_stat.json

                n_m/

                    n_most_freq_urls.json
                    n_most_freq_url_uuids.json
                    
                    {datetime}/
                        m_uuids.json

                        results/
                            url_i_raw.txt
                            url_i_clean.txt
                            url_i_diff.txt
    Option 2:
    Collects M random uuids across all the articles in the input chunk, structure of results is similar:
    repr_experiments/

            {chunk_path}/

                uuids.json

                m/              
                    {datetime}/
                        m_uuids.json

                        results/
                            url_i_raw.txt
                            url_i_clean.txt
                            url_i_diff.txt
    """
    # run an experiment
    exp_datetime = dt.now().strftime("%Y-%m-%dT%H-%M-%S")

    # OPTION 1 (uncomment)
    # # prepare for experiments with m, n and this chunk
    # collect_urls_stat(chunk_path_rel)

    # get_most_frequent(chunk_path_rel, N, M)

    # collect_specific_uuids(chunk_path_rel, N, M)

    # get_random_samples(chunk_path_rel, exp_datetime, N, M)

    # collect_m_texts_by_uuids(chunk_path_rel, exp_datetime, N, M)

    # OPTION 2 (uncomment)
    collect_uuids(chunk_path_rel)

    get_random_samples_from_all(chunk_path_rel, M, exp_datetime)

    collect_m_texts_by_uuids_from_all(chunk_path_rel, exp_datetime, M)


run(CUR_CHUNK_REL_PATH)