# -*- coding: utf-8 -*-
'''
    Date: Mar 20th, 2017

    Author: Guilherme Varela

   updates person uri from old to new

'''

import re
import pandas as pd
import json

PREFIX = 'http://www.seliganapolitica.org/resource/'


def _get_parties():
    '''
        Creates a parties dataframe
    '''
    # upload parties
    parties_d = {}
    with open('datasets/raw/partidos_apicamarav2.json', mode='r') as f:
        parties = json.load(f)['dados']
    f.close()

    for i, party in enumerate(parties):
        for key, value in party.items():
            if not(key in parties_d):
                parties_d[key] = {}
            parties_d[key][i] = value

    df_tmp = pd.DataFrame.from_dict(parties_d)
    df_tmp = df_tmp.set_index('uri')

    df = pd.read_csv('resource_uri/party_resource_uri', sep=';')
    df = df.rename(columns={'Sigla': 'sigla'})
    df = df.join(df_tmp, on='sigla')
    return df


def _get_persons():
    '''
        Creates a persons dataframe
    '''
    df = pd.read_csv('resource_uri/person_resource_uri', sep=';')
    return df


def transform_file():
    # upload person
    # upload party
    df_parties = _get_parties()

    re_triple = re.compile(r'<(.*?)>')
    re_post = re.compile(r'postIn')
    re_uri = re.compile(r('{:}(.*?)$'.format(PREFIX)))
    file_path = 'updates/in/deputados-55.rdf'
    with open(file_path, mode='r') as f:
        line = f.readline()
        txt = line

        _subject = re_triple.match(txt).groups()[0]
        length = len(_subject) + 3
        _predicate = re_triple.match(txt[l:]).groups()[0]
        length += len(_predicate) + 3
        _object = re_triple.match(txt[l:]).groups()[0]
        # subject is a foaf agent, object is party
        if re_post(_predicate):
           party_api_id= re_uri.match(_object)


        print(_subject, _predicate, _object)


if __name__ == '__main__':
    transform_file()
