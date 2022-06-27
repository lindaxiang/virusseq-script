#!/usr/bin/env python3

import requests
import json
import os
from argparse import ArgumentParser
from datetime import date

def download(song_url, file_dump, study_list):
    dump_dir = os.path.dirname(file_dump)
    if not os.path.exists(dump_dir):
        os.makedirs(dump_dir)
    
    date_str = date.today().strftime("%Y-%m-%d")
    if study_list:
      study_str = '_'.join(study_list)
    else:
      study_str = 'all'
    dump_file = '.'.join([file_dump, study_str, date_str, 'jsonl'])

    response = requests.get(song_url + '/studies/all')
    if response.status_code == 200:
        studies = response.json()
    else:
        raise Exception(response.text)

    pageSize = 100
    with open(dump_file, 'w') as fp:
        for study in studies:
            if study_list and not study in study_list: continue
            print(study)
            response = requests.get(song_url + ('/entities?page=1&projectCode=%s&size=%s' % (study, pageSize))).json()
            totalPages = response['totalPages']
            print("totalPages: "+str(totalPages))
            pageList = list(range(0,totalPages))
            for p in pageList:             
              print(p)
              response = requests.get(song_url + ('/entities?page=%s&projectCode=%s&size=%s' % (p, study, pageSize))).json()
              analyses = response['content']
              for analysis in analyses:
                analysisId = analysis.get('gnosId')
                response = requests.get(song_url + ('/studies/%s/analysis/%s' % (study, analysisId))).json()
                if not response['analysisState'] == "PUBLISHED": continue
                fp.write(json.dumps(response)+"\n")

    return

def main():
    parser = ArgumentParser()
    parser.add_argument("-m", "--song_url", dest="song_url", type=str, default="https://song.dev.cancogen.cancercollaboratory.org", help="SONG URL")
    parser.add_argument("-d", "--file_dump", dest="file_dump", type=str, default="data/virusseq-song", help="file dump basename and path")
    parser.add_argument("-s", "--study", dest="study", nargs='+', help="list of studies")
    args = parser.parse_args()

    download(args.song_url, args.file_dump, args.study)

if __name__ == "__main__":
    main()
