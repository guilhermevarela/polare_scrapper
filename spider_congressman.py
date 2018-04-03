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

import xml.etree.ElementTree as ET

import re

#resource_uri generation and testing
from resource_uri.getters import get_congressmen_uri_by_apiid


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


class CongressmenWithLegislaturaSpider(scrapy.Spider):
    name = 'congressman_with_legislatura'


    # Overwrites default:ASCII
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    def __init__(self, legislatura=55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        self.db_congressmen_uri = get_congressmen_uri_by_apiid()
        self.parse_fn = lambda x: re.sub(r'\n| ', '', str(x))
        self.legislatura = 55

    def start_requests(self):
        '''
            Querying every congressmen during term (legislatura)
        '''
        url = URL_OPEN_DATA_CAMARA_API_V1
        url = '{:}ObterDeputados?numLegislatura={:}'.format(url, self.legislatura)
        req = scrapy.Request(
            url,
            self.request_congressman,
            headers={'accept': 'application/json'}
        )
        yield req

    def request_congressman(self, response):
        '''
           Start by querying every congressmen during term (legislatura)

        '''
        root = ET.fromstring(response.body_as_unicode())
        for deputado in root:
            resource_idx = int(deputado.find('ideCadastro').text)
            url = URL_OPEN_DATA_CAMARA_API_V1
            url = '{:}ObterDetalhesDeputado?ideCadastro='.format(url)
            url = '{:}{:}'.format(url, resource_idx)
            url = '{:}&numLegislatura={:}'.format(url, self.legislatura)

            resource_uri = self.db_congressmen_uri.get(resource_idx, None)
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

        _info = {}
        _info['slp:old_resource_uri'] = registration_uri
        _info['slp:resource_uri'] = str(uuid4())

        for deputados in root:
            for deputado in deputados:
                # for attr in deputado:
                if deputado.tag not in IGNORE_TAGS:
                    key = 'cam:{:}'.format(deputado.tag)
                    value = self.parse_fn(deputado.text)
                    if (len(value) > 0):
                        if 'data' in deputado.tag:
                            yyyy = deputado.text[6:]
                            mm = deputado.text[3:5]
                            dd = deputado.text[:2]
                            _info[key] = '{:}-{:}-{:}'.format(yyyy, mm, dd)
                        else:
                            _info[key] = text_format(deputado.text)
                    else:
                        _info[key] = None
        yield _info


def text_format(txt):
    '''
        Formats before storing
    '''
    # Single spaces between words
    return re.sub(r'  ', ' ', txt)
