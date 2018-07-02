# -*- coding: utf-8 -*-
'''
    Date: Apr 03th, 2018

    Author: Guilherme Varela

'''
import re
import pandas as pd


def date_format(txt):
    result = txt
    if len(txt) == 10:
        yyyy = txt[6:]
        mm = txt[3:5]
        dd = txt[:2]

        result = '{:}-{:}-{:}'.format(yyyy, mm, dd)
    return result


def text_format(txt):
    '''
        Formats before storing
    '''
    # Single spaces between words
    return re.sub(r'  ', ' ', str(txt))


def parse_fn(txt):
    return re.sub(r'\n| ', '', str(txt))


def get_agents():
    '''
        Provides a dictionary with keys being api ids 
        OUTPUT
            dict<key<int>,value<string>>: dictionary
                key: ideCadastro api v1 camara
                value: resource uri
    '''
    resources_path = 'datasets/slp/agents.csv'
    df = pd.read_csv(
        resources_path, sep=';', index_col=0, header=0, encoding='utf-8'
    )
    return df.to_dict()


def get_party():
    '''
        Provides a dictionary with keys being api ids 
        OUTPUT
            dict<key<int>,value<string>>: dictionary
                key: ideCadastro api v1 camara
                value: resource uri
    '''
    result = get_resource(
        'slp/organizations/parties-tse.csv', 'sigla', 'slp:resource_uri'
    )

    return result


def get_role():

    result = get_resource('slp/roles.csv', 'rdfs:label', 'resource_uri')

    return result


def get_resource(table, key_column, resource_uri_column):
    '''
        Provides a dictionary with key=key_column and value=resource_uri

        INPUT
        table<string>
        key_column<string>: name of the column that wil become the key

        resource_uri_column<string>:a string representing the name of a column 

        OUTPUT
            dict<key<int>,polare_resource_uri<string>>: dictionary
    '''
    #Current dir will be where the spiders are
    resource_path = 'datasets/' + table    
    df = pd.read_csv(
        resource_path, sep=';', index_col=None, header=0, encoding='utf-8'
    )

    result = {
        key:value
            for key, value in
                zip(df[key_column],df[resource_uri_column])}

    return result

