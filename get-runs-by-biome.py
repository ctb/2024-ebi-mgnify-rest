#! /usr/bin/env python
import sys
import argparse
import os
from pickle import dump, load

from jsonapi_client import Session as APISession
from jsonapi_client import Modifier
import requests
import pandas as pd
# Data transformation
from functools import reduce
from collections import defaultdict

TEST=False


def get_all_biome_names():
    endpoint_name='biomes'
    r = requests.get(f"https://www.ebi.ac.uk/metagenomics/api/latest/{endpoint_name}")
    result = r.json()
    num_pages = result['meta']['pagination']['pages']

    if TEST:
        print('TEST MODE: setting num pages for biome names to 2')
        num_pages = 2

    xx = []
    for page in range(1, num_pages + 1):
        print(f'working on page: {page}')
        r = requests.get(f"https://www.ebi.ac.uk/metagenomics/api/latest/{endpoint_name}?page={page}")
        xx.append(r.json())

    print(f'got {len(xx)} biome pages')
    return xx

def get_samples_for_biome(biome):
    endpoint_name='biomes'
    r = requests.get(f"https://www.ebi.ac.uk/metagenomics/api/latest/{endpoint_name}/{biome}/samples")
    result = r.json()
    num_pages = result['meta']['pagination']['pages']

    if TEST:
        print('TEST MODE: setting num pages for samples per biome to 2')
        num_pages = 2

    xx = []
    for page in range(1, num_pages + 1):
        print(f'working on page: {page} of {num_pages}')
        r = requests.get(f"https://www.ebi.ac.uk/metagenomics/api/latest/{endpoint_name}/{biome}/samples?page={page}")
        xx.append(r.json())

    print(f'got {len(xx)} pages for {biome}')
    return xx


def get_runs_from_samples(samples_data):
    xx = []
    for item in samples_data:
        for item2 in item['data']:
            xx.append(item2['relationships']['runs']['links']['related'])
    return xx


def get_run_info_for_runs(run_urls):
    zz = []
    for n, run_url in enumerate(run_urls):
        print(f"{n + 1} of {len(run_urls)}")
        if TEST and n > 1:
            print('TEST MODE: exiting after 2 run info retrievals.')
            break

        r = requests.get(run_url)
        result = r.json()
        num_pages = result['meta']['pagination']['pages']
        if num_pages == 1:
            zz.append(result)
        else:
            for page in range(1, num_pages + 1):
                print(f'working on page: {page} of {num_pages} for run URL {run_url}')        
                r = requests.get(run_url + f"?page={page}")
                zz.append(r.json)

    return zz


def read_pickle(filename):
    if os.path.exists(filename):
        print(f"reading from '{filename}'")
        with open(filename, 'rb') as fp:
            x = load(fp)
            return x

    return None


def save_pickle(x, filename):
    with open(filename, 'wb') as fp:
        dump(x, fp)


def main():
    p = argparse.ArgumentParser()
    args = p.parse_args()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    ##
    ## First get a list of all the biomes.
    ##

    # does it exist? if so, read. if not, grab, then save.
    biome_filename = '1-biomes.pickle'
    biome_json = read_pickle(biome_filename)
    if not biome_json:
        biome_json = get_all_biome_names()
        save_pickle(biome_json, biome_filename)

    all_biomes = []
    for result in biome_json:
        for record in result['data']:
            biome_name = record['id']
            sample_count = record['attributes']['samples-count']
            all_biomes.append((biome_name, sample_count))

    # sort and filter
    all_biomes.sort(key=lambda x: x[1])
    print("all:", len(all_biomes))
    all_biomes = [ (x, y) for (x, y) in all_biomes if y > 0 ]
    print("0-filtered:", len(all_biomes))
    #all_biomes = [ (x, y) for (x, y) in all_biomes if y >= 10 ]
    #print("10-filtered:", len(all_biomes))
    #all_biomes = [ (x, y) for (x, y) in all_biomes if y >= 50 ]
    #print("50-filtered:", len(all_biomes))

    # select only biomes that have four parts
    all_biomes = [ (x, y) for (x, y) in all_biomes if x.count(':') == 3 ]
    print("hierarchy filtered:", len(all_biomes))

    ##
    ## Then, for each biome, get a list of associated samples.
    ##

    biome_samples_filename = '2-biome-samples.pickle'
    samples_by_biome = read_pickle(biome_samples_filename)
    if not samples_by_biome:
        samples_by_biome = {}

    for n, (biome_name, count) in enumerate(all_biomes):
        if TEST and n > 10:
            print(f'TEST MODE stopping at {n} biomes for testing purposes')
            break

        if biome_name not in samples_by_biome:
            print(f"getting: {biome_name} ({n + 1} of {len(all_biomes)})")
            samples_by_biome[biome_name] = get_samples_for_biome(biome_name)
            print(f"got: {len(samples_by_biome[biome_name])}")

            # save each time!
            save_pickle(samples_by_biome, biome_samples_filename)

    ##
    ## Extract the list of runs from each biome (no web request needed)
    ##

    runs_by_biome = defaultdict(list)
    for biome_name, samples_vv in samples_by_biome.items():
        runs = get_runs_from_samples(samples_vv)
        runs_by_biome[biome_name].extend(runs)

    runinfo_by_biome_filename = '3-runinfo-by-biome.pickle'
    runinfo_by_biome = read_pickle(runinfo_by_biome_filename)
    if not runinfo_by_biome:
        runinfo_by_biome = {}

    for biome_name, runlist in runs_by_biome.items():
        if biome_name not in runinfo_by_biome:
            print(f"working on {biome_name} - {len(runlist)} runs")
            zz = get_run_info_for_runs(runlist)
            runinfo_by_biome[biome_name] = zz

            # again, save each round!
            save_pickle(runinfo_by_biome, runinfo_by_biome_filename)

    ##
    ## For each biome, now parse out the information. No Web requests needed.
    ##

    for biome_name, run_info in runinfo_by_biome.items():
        for n, ri in enumerate(run_info):
            try:
                for item in ri['data']:
                    attr = item.get('attributes')
                    print(biome_name, attr['accession'], attr['experiment-type'], attr['instrument-platform'], attr['instrument-model'])
            except TypeError:
                print(f'ERROR: biome {biome_name}, entry {n}')
                raise


if __name__ == '__main__':
    sys.exit(main())
