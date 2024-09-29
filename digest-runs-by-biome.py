#! /usr/bin/env python
import sys
import argparse
import os
from pickle import dump, load
from pprint import pprint
import collections

from jsonapi_client import Session as APISession
from jsonapi_client import Modifier
import requests
import pandas as pd
# Data transformation
from functools import reduce
from collections import defaultdict

TEST=True


def get_runs_from_samples(samples_data):
    xx = []
    for item in samples_data:
        for item2 in item['data']:
            xx.append(item2['relationships']['runs']['links']['related'])
    return xx


def read_pickle(filename):
    if os.path.exists(filename):
        print(f"reading from '{filename}'")
        with open(filename, 'rb') as fp:
            x = load(fp)
            return x

    return None


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

    ##
    ## Extract the list of runs from each biome (no web request needed)
    ##

    runs_by_biome = defaultdict(list)
    for biome_name, samples_vv in samples_by_biome.items():
        runs = get_runs_from_samples(samples_vv)
        runs_by_biome[biome_name].extend(runs)

    runinfo_by_biome_filename = '3-runinfo-by-biome.pickle'
    runinfo_by_biome = read_pickle(runinfo_by_biome_filename)

    ##
    ## For each biome, now parse out the information. No Web requests needed.
    ##

    experiment_types = collections.Counter()
    platforms = collections.Counter()
    models = collections.Counter()
    for biome_name, run_info in runinfo_by_biome.items():
        for n, ri in enumerate(run_info):
            try:
                for item in ri['data']:
                    attr = item.get('attributes')
                    if attr['experiment-type'] == 'metagenomic' and attr['instrument-platform'] == 'ILLUMINA':
                        print((biome_name, attr['accession'], attr['experiment-type'], attr['instrument-platform'], attr['instrument-model']))
                    experiment_types[attr['experiment-type']] += 1
                    platforms[attr['instrument-platform']] += 1
                    models[attr['instrument-model']] += 1
            except TypeError:
                print(f'ERROR: biome {biome_name}, entry {n}')

    #pprint(experiment_types.most_common(10))
    #pprint(platforms.most_common(5))
    #pprint(models.most_common(5))
    


if __name__ == '__main__':
    sys.exit(main())
