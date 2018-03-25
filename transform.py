# -*- coding: utf-8 -*-
'''
    Date: Mar 20th, 2017

    Author: Guilherme Varela

   updates person uri from old to new

'''

import re
import pandas as pd
import json


def get_parties():
    '''
        Gets all parties related fields merging data from
            parties_apicamarav2.json
            internally generated uris slp
            tse

        returns:
            df containing the parties
                all columns are <providers>:<field_name>
    '''
    # upload slp parties
    df = pd.read_csv('resource_uri/party_resource_uri.csv', sep= ';', index_col= None)
    df = df.rename(columns={'Nome':'name', 'party_resource_uri':'resource_uri'})
    prov = ['slp']*len(df.columns)
    # import code; code.interact(local=dict(globals(), **locals()))
    new_index = pd.MultiIndex.from_tuples(zip(prov, df.columns.names))

    # upload camara api parties
    parties_d = {}
    with open('datasets/raw/parties_apicamarav2.json', mode='r') as f:
        parties = json.load(f)['dados']
    f.close()

    for i, party in enumerate(parties):
        for key, value in party.items():
            if not(key in parties_d):
                parties_d[key] = {}
            parties_d[key][i] = value

    _df = pd.DataFrame.from_dict(parties_d)
    _df = df_parties.set_index('uri')

    df  = df.join(_df, on= 'sigla', how= 'left')
    
    return df_parties


def transform_file():
    #upload person    
    df_parties = get_parties()

    re_triple = re.compile(r'<(.*?)>')
    re_post   = re.compile(r'postIn')
    file_path = 'updates/in/deputados-55.rdf'
    with open(file_path, mode='r') as f:
        line = f.readline()
        txt = line

        
        _subject = re_triple.match(txt).groups()[0]        
        l = len(_subject) + 3
        _predicate = re_triple.match(txt[l:]).groups()[0]
        l += len(_predicate) + 3
        _object = re_triple.match(txt[l:]).groups()[0]
        
        if re_post.search(_predicate):
            # subject is a foaf agent, object is party            
            print('foaf:agent')
        


        print(_subject, _predicate, _object)


if __name__ == '__main__':
    transform_file()
