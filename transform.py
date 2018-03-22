# -*- coding: utf-8 -*-
'''
    Date: Mar 20th, 2017

    Author: Guilherme Varela

   updates person uri from old to new

'''

import re
import pandas as pd
import json

def transform_file():
    #upload person

    #upload parties
    parties_d={}
    with open('datasets/raw/partidos_apicamarav2.json', mode='r') as f:
        parties = json.load(f)['dados']
    f.close()

    for i, party in enumerate(parties):
        for key, value in party.items():
            if not(key in parties_d):
                parties_d[key] = {}
            parties_d[key][i] = value

    df_parties =  pd.DataFrame.from_dict(parties_d)
    df_parties = df_parties.set_index('uri')

        
    import code; code.interact(local=dict(globals(), **locals()))
    # parties = json.loads('datasets/raw/partidos_apicamarav2.json')
    
    df_parties = df_parties.set_index('id')
    import code; code.interact(local=dict(globals(), **locals()))

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
        # if re_post(_predicate):
        #subject is a foaf agent, object is party
        


        print(_subject, _predicate, _object)


if __name__ == '__main__':
    transform_file()
