import requests
from bs4 import BeautifulSoup
import argparse
from urllib.parse import urlparse
import subprocess
import os
import tempfile
import json
import sys
import pandas as pd
import pickle
import hashlib
from tqdm.auto import tqdm 

cache = {}

def get_raw_file_content(url):
    """
    Access a website, find the 'raw file' button link, and fetch its content.
    
    Args:
        url (str): The URL of the website containing the 'raw file' button
        
    Returns:
        str: Content of the raw file, or None if not found
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        raw_link = soup.find('a', class_="btn btn-secondary btn-sm swh-tr-link").get('href')
        
        if (raw_link == '') or (not raw_link):
            print("Could not find a 'raw file' link on the page.", file=sys.stderr)
            return None
        
        base_url = "{0.scheme}://{0.netloc}".format(urlparse(url))
        raw_link_full = base_url + raw_link
        
        raw_response = requests.get(raw_link_full, headers=headers)
        raw_response.raise_for_status()
        
        return raw_response.text
        
    except requests.exceptions.RequestException as e:
        print(f"Error accessing the website: {e}", file=sys.stderr)
        return None
    
def detect_license(license):
    """
    Analyze content using an external license detection tool and extract the 'license_detections' field.
    
    Args:
        license (str): File content to analyze
        
    Returns:
        dict: License detections information or None if detection failed
    """
    if not license:
        return None
    
    h = hashlib.sha1(license.encode('utf-8')).hexdigest()
    if h in cache:
        return cache[h]
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
        temp.write(license)
        temp_path = temp.name
    
    try:
        result = subprocess.run(
            f"scancode -l -n 30 --json - {temp_path}",
            shell=True,
            text=True,
            capture_output=True
        )
        if result.returncode != 0:
            print(f"License detection failed: {result.stderr}")
            return None
        
        try:
            json_data = json.loads(result.stdout)

            if 'files' in json_data and len(json_data['files']) > 0:
                matches = {}
                max_score = [0, 0]
                for licence in json_data['files'][0]['license_detections']:
                    for match in licence['matches']:
                        matches[(match['score'], match['matched_length'])] = match['license_expression']
                        if match['score'] > max_score[0]:
                            max_score[0] = match['score']
                            max_score[1] = match['matched_length']
                        elif match['score'] == max_score[0]:
                            max_score[1] = max(max_score[1], match['matched_length'])
                if max_score != [0, 0]:
                    license_scanned = matches[(max_score[0], max_score[1])]
                    cache[h] = license_scanned
                    return license_scanned
            return None
        except json.JSONDecodeError:
            print("Failed to parse license detection output as JSON")
            return None
            
    finally:
        os.unlink(temp_path)

def single_url():
    parser = argparse.ArgumentParser(description='Fetch and analyze raw file content from a website.')
    parser.add_argument('url', help='URL of the website containing the raw file button')
    args = parser.parse_args()
    
    content = get_raw_file_content(args.url)
    
    if content: 
        print(content)
    else:
        print("Failed to retrieve raw content.", file=sys.stderr)
        
def main():
    ds = pd.read_csv("../results/repos_modified_path.csv", delimiter=';')
    ds.drop_duplicates().reset_index()
    
    tqdm.pandas()
    ds['Rev-License-Scanned'] = ds['Rev-License-Path'].progress_apply(lambda p: detect_license(get_raw_file_content(p)) if p else None)
    with open("../results/results_full_part1.pkl", 'wb') as f:
        pickle.dump(ds, f)
        
    ds['Snap-License-Scanned'] = ds['Snap-License-Path'].progress_apply(lambda p: detect_license(get_raw_file_content(p)) if p else None)
    with open("../results/results_full_part2.pkl", 'wb') as f:
        pickle.dump(ds, f)

if __name__ == "__main__":
    main()