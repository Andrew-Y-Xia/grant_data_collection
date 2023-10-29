import os
import time

import numpy as np
import pandas as pd

import dimcli

import pickle
import json

import datetime
import requests

from urllib.error import HTTPError

from itertools import islice


dimcli.login(key=os.environ['DIMENSIONS_API'],  
                 endpoint="https://app.dimensions.ai/api/dsl/v2")
dsl = dimcli.Dsl()


"""
UTIL START
"""

# Generator that returns a 
def gen_dates(start_year, end_year):
    x = datetime.datetime(start_year, 1, 1)
    end = datetime.datetime(end_year, 1, 1)
    while x < end:
        yield x.strftime("%Y-%2m-%2d"), (x + datetime.timedelta(days=30)).strftime("%Y-%2m-%2d")
        x += datetime.timedelta(days=30)

        
# Helper function to split iterables into batches
def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

"""
UTIL END

DIMENSIONS COLLECTION START
"""
        
def dim_full_researcher_gen():
    for i in range(1900, 2024): 
        for researcher in dsl.query_iterative("search researchers where obsolete=0 and research_orgs.country_code=\"US\"  and first_grant_year=" + str(i) + " return researchers[id]", verbose=False).researchers:
            yield researcher['id']
    for i in range(1900, 2024): 
        for researcher in dsl.query_iterative("search researchers where obsolete=0 and research_orgs.country_code=\"CN\" and first_grant_year=" + str(i) + " return researchers[id]", verbose=False).researchers:
            yield researcher['id']

def dim_gen_full_grants():
    
    researchers = dim_full_researcher_gen()
    for researcher_ids in split_every(500, researchers):
        # Query grants data
        data = None
        while data == None:
            try:
                data = dsl.query_iterative("""
                    search grants 
                    where researchers in """ + json.dumps(researcher_ids) + """
                    return grants[investigators+researchers+id+title+active_year+start_date+end_date+research_org_names+research_org_countries+funding_usd+funding_org_name+funder_org_countries+project_numbers]""",
                                verbose = False, limit = 800)
            except requests.exceptions.HTTPError:
                print("HTTPError")
                time.sleep(1)
                continue
        if not hasattr(data, "grants"):
            continue
        for grant in data.grants:
            try:
                pass
                # grant["researchers"] = clean_people(grant["researchers"])
                # grant["investigators"] = clean_people(grant["investigators"])
            except KeyError:
                continue
        for grant in data.grants:
            yield grant
        time.sleep(1)     

def researcher_with_ppid_gen():
    for i in range(1900, 2024): 
        for researcher in dsl.query_iterative("search researchers where obsolete=0 and nih_ppid is not empty and first_grant_year=" + str(i) + " return researchers[id+nih_ppid]", verbose=False).researchers:
            yield researcher['id'], researcher['nih_ppid']
        
def extract_dimensions_data():
    for i, grant_batch in enumerate(split_every(100000, dim_gen_full_grants())):
        pickle.dump(grant_batch, open("full_dimensions" + str(i) + ".p", "wb"))
        print("dimensions batch " + str(i));
    
    dimensions_id_to_nih_id = {}
    for dim_id, nih_id in researcher_with_ppid_gen():
        dimensions_id_to_nih_id[dim_id] = nih_id
    nih_id_to_dimensions_id = {}

    for key, value in dimensions_id_to_nih_id.items():
        for nih_id in value:
            nih_id_to_dimensions_id[nih_id] = key

    pickle.dump((dimensions_id_to_nih_id, nih_id_to_dimensions_id), open("dim_nih_ids.p", "wb"))


        
"""
DIMENSIONS COLLECTION END

NIH COLLECTION START
"""


verbose = True
def get_nih_grants_from(start_date, to_date):
    offset = 0
    params = { 'criteria': { "project_start_date": { "from_date": start_date, "to_date": to_date }}, "limit": 500, "offset": offset}
    resp = requests.post('https://api.reporter.nih.gov/v2/projects/search', json=params)
    time.sleep(1.1)
    try:
        json = resp.json()
    except requests.JSONDecodeError:
        return []
    if type(json) == list:
        print(json)
    num_records = json['meta']['total']
    if num_records >= 15000:
        print("too many records")
    grants = [g for g in json['results']]
    if num_records == 0:
        return []
    while num_records >= 500:
        offset += 500
        params = { 'criteria': { "project_start_date": { "from_date": start_date, "to_date": to_date }}, "limit": 500, "offset": offset}
        resp = requests.post('https://api.reporter.nih.gov/v2/projects/search', json=params)
        time.sleep(1.1)
        try:
            json = resp.json()
        except requests.JSONDecodeError:
            continue
        grants += [g for g in json['results']]
        num_records -= 500
    for grant in grants:
        del grant['abstract_text']
    return grants

def nih_grants_gen():
    for start, end in gen_dates(1970, 2024):
        for grant in get_nih_grants_from(start, end):
            yield grant
    
def extract_nih_data():
    for i, grant_batch in enumerate(split_every(25000, nih_grants_gen())):
        pickle.dump(grant_batch, open("nih_recrawl" + str(i) + ".p", "wb"))
        print("nih batch " + str(i))

"""
NIH COLLECTION END
"""

