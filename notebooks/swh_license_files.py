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
import duckdb
import time

cache = {}

def generate_swh_url(swhid, path, branch=None):
    """
    Generate a Software Heritage archive URL from a revision ID and path.
    
    Args:
        swhid: A Software Hash ID (SWHID) of a revision or a snapshot
        path: A file path, possibly with leading slashes
        
    Returns:
        A URL to the file in the Software Heritage archive
        
        None if the swhid is incorrect
    """

    clean_path = path
    if path.startswith('./'):
        clean_path = path[2:]
    elif path.startswith('/'):
        clean_path = path[1:]
    
    if "swh:1:rev:" in swhid:
        hash_part = swhid[swhid.find("swh:1:rev:") + 10:]
        return f"https://archive.softwareheritage.org/browse/revision/{hash_part}/?path={clean_path}"
    elif "swh:1:snp:" in swhid:
        hash_part = swhid[swhid.find("swh:1:snp:") + 10:]
        return f"https://archive.softwareheritage.org/browse/snapshot/{hash_part}/directory/?branch={branch}&path={clean_path}"
    return None

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
            if 'license_detections' in json_data and len(json_data['license_detections']) > 0:
                matches = {}
                max_score = 0
                for license in json_data['license_detections']: 
                    
                    for match in license['reference_matches']:
                        max_score = max(max_score, match['score'])
                        if match['license_expression'] not in matches:
                            matches[match['license_expression']] = match['score']
                        else:
                            matches[match['license_expression']] = max(matches[match['license_expression']], match['score'])
                if max_score < 80:
                    return "Undetermined"
                results = []
                for (l, s) in matches.items():
                    if s == max_score:
                        results.append(l)
                results.sort()
                result_str = ", ".join(results)
                return result_str
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
        
def main(df):
    tqdm.pandas()
    df['Rev-License-Scanned'] = df['Rev-License-Path'].progress_apply(lambda p: detect_license(get_raw_file_content(p)) if p else None)
    with open("../../data/license_full_part1.pkl", 'wb') as f:
        pickle.dump(df, f)
        
    df['Snap-License-Scanned'] = df['Snap-License-Path'].progress_apply(lambda p: detect_license(get_raw_file_content(p)) if p else None)
    with open("../../data/license_full_part2.pkl", 'wb') as f:
        pickle.dump(df, f)

if __name__ == "__main__":
    df = pd.DataFrame()
    with open('../../data/license_files_with_path.pkl', 'rb') as f:
        df = pickle.load(f)
    main(df)