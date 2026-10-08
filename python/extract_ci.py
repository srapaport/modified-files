from swh_license_files import get_raw_file_content
import os
import time
import pickle
import pandas as pd
import hashlib

def save_raw_file_content(url1, url2, swhid, path):
    raw1 = get_raw_file_content(url1)
    raw2 = get_raw_file_content(url2)
    if not raw1 or not raw2:
        return None

    filename = path.split('/')[-1]
    url_hash = hashlib.md5(url1.encode()).hexdigest()
    dir_path = f"../results/ci_files/{swhid}_{url_hash}"
    os.makedirs(dir_path, exist_ok=True)

    with open(f"{dir_path}/{filename}.original", "w") as f:
        f.write(raw1)

    with open(f"{dir_path}/{filename}.modified", "w") as f:
        f.write(raw2)

    return raw1, raw2
    

def save_all_raws(df):
    amount_of_error = 0
    for index, row in df.iterrows():
        original_url = row['Rev-CI-Path']
        altered_url = row['Snap-CI-Path']
        swhid = row['revision']
        path = row['path']
        if not save_raw_file_content(original_url, altered_url, swhid, path):
            amount_of_error += 1
        time.sleep(6)
    print(f'amount of row that weren\'t read: {amount_of_error:,}')
    

if __name__ == "__main__":
    start = time.time()
    df = pd.DataFrame
    with open('../results/CI_v3_with_paths.pkl', 'rb') as f:
        df = pickle.load(f)
    save_all_raws(df)
    print(f'time elapsed: {time.time() - start} seconds')
