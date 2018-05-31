import os
import re
import glob
import pandas as pd
import json
import sys
sys.path.append('..')
sys.path.append('scripts')
# import os.path
import errno

from uri_generators import person_uri, formaleducation_uri
from uuid import uuid4


class Migrator(object):
    '''
        Provides migration utilities in order to convert previous 
            migration schemes into new objects
    '''

    def __init__(self):
        self._initialize_agents()
        self._initialize_formaleducation()

    def migrate(self, input_file):
        '''
            Migrates file
                * Replace uris for the following resources:
                    * Person
                    * Organization

                * Remove all lines which have uris for resources:
                    * Membership
                    * Post

                * Add instances ttl lines for resources:
                    * Membership
                    * Post
        '''
        *input_dir, filename = input_file.split('/')
        input_dir = '/'.join(input_dir)
        input_glob = glob.glob('{:}/*txt'.format(input_dir))

        output_dir = 'datasets/migrations/outputs/'
        for txtfile in input_glob:
            filename = txtfile.split('/')[-1]
            print('processing .. {:}'.format(filename))

            with open(txtfile, mode='r') as f:
                txt = f.read()
            f.close()

            for oldidx, newidx in self.agents.items():
                txt = txt.replace(oldidx, newidx)

            for oldidx, newidx in self.formaleducation.items():
                # import code; code.interact(local=dict(globals(), **locals()))
                txt = txt.replace(oldidx, newidx[-1])

            if not os.path.exists(os.path.dirname(output_dir)):
                try:
                    os.makedirs(os.path.dirname(output_dir))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

        output_path = '{:}{:}'.format(output_dir, filename)
        with open(output_path, mode='w') as f:
            f.write(txt)
        f.close()
        print('saved at ', output_path)

    def _initialize_agents(self):
        '''
            Computes an dictionary olduri --> newuri
        '''
        _df = pd.read_csv('datasets/slp/agents.csv', sep=';', encoding='utf-8', index_col=0)
        print(_df.columns)
        _df = _df[['cam:nomeCivil', 'cam:dataNascimento']]


        _fullname = _df['cam:nomeCivil'].to_dict()
        _birthdate = _df['cam:dataNascimento'].to_dict()

        self.agents = {
            person_uri(_fullname[idx], _birthdate[idx]): idx
            for idx in _fullname  if _fullname[idx] and isinstance(_fullname [idx], str)
        }

    def _initialize_formaleducation(self):
        '''
            Computes an dictionary olduri --> newuri
        '''
        formaleducation_path = 'datasets/migrations/mappings/formaleducation.json'
        # import code; code.interact(local=dict(globals(), **locals()))
        if os.path.isfile(formaleducation_path):
            with open(formaleducation_path, mode='r') as f:
                educ_dict = json.load(f)
            f.close()
        else:
            educ_dict = {}

        tags = '<escolaridade>(.*?)</escolaridade>'
        finder = lambda x : re.findall(tags, x)
        educ_set = set()
        for g in glob.glob('datasets/migrations/xml/*'):
            with open(g, mode='r') as f:
                contents = f.read()
            f.close()
            educ_set = set(finder(contents)).union(educ_set)

        educ_list = list(educ_set)
        for i in educ_list:
            if not i in educ_dict:
                # http://www.seliganapolitica.org/resource/skos/Formacao#
                olduri = re.sub('http://www.seliganapolitica.org/resource/skos/Formacao#','',formaleducation_uri(i))
                educ_dict[i] = (olduri, str(uuid4()))

        with open(formaleducation_path, mode='w') as f:
            json.dump(educ_dict, f)
        f.close()


        self.formaleducation = educ_dict


class MapperPerson(object):
    def __init__(self):
        '''
            Loads the old and new uri schemas for resource 
        '''
        pass

    def map(self, uriold):
        '''
            maps an uri (old) to a new one

            args:
                uriold : string old uri

            returns:
                urinew : string newly generated uri
        '''
        pass


class MapperOrg(object):
    def __init__(self):
        '''
            Loads the old and new uri schemas for resource 
        '''
        pass

    def map(self, uriold):
        '''
            maps an uri (old) to a new one

            args:
                uriold : string old uri

            returns:
                urinew : string newly generated uri
        '''
        pass


if __name__ == '__main__':
    Migrator().migrate('datasets/migrations/rdf/deputados-info-legislatura-55.txt')
    