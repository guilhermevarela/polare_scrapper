# -*- coding: utf-8 -*-
'''
    Date: Mar 31th, 2018

    Author: Guilherme Varela

    ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

    Three phase strategy:
    1. Get each congressmen for current term
    2. For each congressmen get details
    3. Within details gets current membership and membershipHistory


    Scrapy shell: 
        scrapy shell 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=160518&numLegislatura=55'

    Scrapy running: 
        scrapy runspider spider_congressman.py

    Scrapy run + store: 
        scrapy runspider spider_congressman.py -o datasets/congressmen-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
'''
import scrapy
import pandas as pd
import xml.etree.ElementTree as ET

import re
from collections import deque 
#resource_uri generation and testing
# from resource_uri.getters import get_congressmen_uri_by_apiid


# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX = 'http://www.seliganapolitica.org/resource/'
URL_OPEN_DATA_CAMARA_API_V1 = 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'


IGNORE_TAGS = set(['numLegislatura', 'gabinete', 'comissoes',
                   'partidoAtual', 'situacaoNaLegislaturaAtual',
                   'periodosExercicio', 'filiacoesPartidarias',
                   'historicoLider', 'historicoNomeParlamentar',
                   'cargosComissoes', 'idParlamentarDeprecated',
                   'ufRepresentacaoAtual'])


class MembershipWithRoleDeputadoSpider(scrapy.Spider):
    name = 'membership_with_role_deputado'


    # Overwrites default:ASCII
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    def __init__(self, legislatura=55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        
        self.congressmen_d = get_congressmen(legislatura)
        self.posts_d = deque(get_posts(legislatura), maxlen=315)
        self.role    = get_role()
        self.parse_fn = lambda x: re.sub(r'\n| ', '', str(x))
        self.legislatura = 55

    def start_requests(self):
        '''
            Stage 1: Request Get each congressmen for current term
        '''
        for resource_idx, resource_uri in self.congressmen_d.items():
            
            url = URL_OPEN_DATA_CAMARA_API_V1
            url = '{:}ObterDetalhesDeputado?ideCadastro='.format(url)
            url = '{:}{:}'.format(url, resource_idx)
            url = '{:}&numLegislatura={:}'.format(url, self.legislatura)

            req = scrapy.Request(
                url,
                self.parse_congressman,
                headers={'accept': 'application/json'},
                meta={'resource_uri': resource_uri,
                      'resource_idx': resource_idx}
            )

            yield req

    def parse_congressman(self, response):
        '''
            Paginates congressman xml

            ref
                http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=160518&numLegislatura=55

        '''
        root = ET.fromstring(response.body_as_unicode())
        registration_uri = response.meta['resource_uri']

        
        for deputados in root:            
            for membership in deputados.find('./periodosExercicio'):                
                for attr in memberships:
                    import code; code.interact(local=dict(globals(), **locals()))
                    
                    
        

def get_congressmen(legislatura):
    file_path = 'datasets/deputados-{:}.csv'.format(legislatura)
    df = pd.read_csv(file_path, sep=';', index_col=None)
    df = df.set_index('cam:ideCadastro')
    df = df['slp:resource_uri']
    return df.to_frame().to_dict()['slp:resource_uri']

def get_posts(legislatura):
    file_path = 'datasets/posts-{:}.csv'.format(legislatura)
    with open(file_path, 'r') as f:
        postsstr = f.read()
    f.close()

    return set(postsstr.split('\n')[1:])

def get_role():
    # Deputado
    return 'b27beba7-ca02-4041-a9e0-1793bcd141fe'
