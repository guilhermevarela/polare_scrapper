'''
    Author: Guilherme Varela
   
    Date: April, 6th

    converts URI from vote_events
'''
import os
import errno

import glob
import pandas as pd

PREFIX = 'www.seliganapolitica.org/resource/'


def get_deputies(deprecated=False):
    '''
        Extracts deputies from agents.csv

        args
        returns:
            dict<str, str>: keys in cam:ideCadastro and values are de uris
    '''
    if deprecated:
        df = pd.read_csv('person_resource_uri.csv', sep=';', header=0,
                         index_col='ideCadastro', encoding='utf-8')
        idx2url = df['resource_uri'].to_dict()
        return idx2url
    else:
        df = pd.read_csv('../slp/agents.csv', sep=';', header=0,
                         index_col='slnp:resource_uri', encoding='utf-8')
        df = df[df['cam:ideCadastro'].notnull()]

        uri2idx = df['cam:ideCadastro'].to_dict()
        idx2uri = {int(idx): uri
                   for uri, idx in uri2idx.items()}

        return idx2uri


def get_mapper():
    '''
        Extracts deputies from person_resources_uri.csv

        args
        returns:
            dict<str, str>: keys in cam:ideCadastro and values are de uris
    '''
    dfrom = get_deputies(deprecated=True)
    dto = get_deputies(deprecated=False)

    return {_uri: dto[_idx] for _idx, _uri in dfrom.items()}


def main():
    xmlfiles = glob.glob('vote_events/*.xml')
    d = get_mapper()

    for xmlfile in xmlfiles:
        filename = xmlfile.split('/')[-1]
        print('processing .. {:}'.format(filename))

        with open(xmlfile, mode='r') as f:
            voteventstr = f.read()
        f.close()

        for oldidx, newidx in d.items():
            voteventstr = voteventstr.replace(oldidx, newidx)

        *dirs, filename = xmlfile.split('/')
        file_path = '../slp/{:}/{:}'.format('/'.join(dirs), filename)
        if not os.path.exists(os.path.dirname(file_path)):
            try:
                os.makedirs(os.path.dirname(file_path))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        with open(file_path, mode='w') as f:
            f.write(voteventstr)
        f.close()
        print('saved at ', file_path)


if __name__ == '__main__':
    main()
