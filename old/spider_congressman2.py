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
        scrapy runspider spider_congressman2.py

    Scrapy run + store:
        scrapy runspider spider_congressman2.py -o datasets/camara2/json/congressmen-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
'''
import scrapy

import json

import aux

# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX = 'http://www.seliganapolitica.org/resource/'
URL_OPEN_DATA_CAMARA_API_V2 = 'https://dadosabertos.camara.leg.br/api/v2/deputados'


IGNORE_TAGS = set(['ultimoStatus', 'situacao', 'urlWebsite'])


class CongressmenWithLegislaturaSpider(scrapy.Spider):
    name = 'congressman_with_legislatura2'


    # Overwrites default: ASCII
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    def __init__(self, legislatura=55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        self.db_congressmen_uri = aux.get_congressmen()
        self.legislatura = 55

    def start_requests(self):
        '''
            Querying every congressmen during term (legislatura)
        '''
        url = URL_OPEN_DATA_CAMARA_API_V2
        url = '{:}?idLegislatura={:}'.format(url, self.legislatura)
        url = '{:}&ordenarPor=nome'.format(url)
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
        body = json.loads(response.body_as_unicode())
        data = body['dados']
        links = body['links']

        for congressman in data:
            resource_idx = int(congressman['id'])
            url = congressman['uri']
            resource_uri = self.db_congressmen_uri.get(resource_idx, None)

            req = scrapy.Request(
                url,
                self.parse_congressman,
                headers={'accept': 'application/json'},
                meta={'resource_uri': resource_uri,
                      'resource_idx': resource_idx}
            )
            yield req

        # Next page
        for link in links:
            if (link['rel'] == 'next'):
                if link['href']:
                    req = scrapy.Request(
                        link['href'],
                        self.request_congressman,
                        headers={'accept': 'application/json'}
                    )
                    yield req

    def parse_congressman(self, response):
        '''
            Paginates congressman xml

            ref
                http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=160518&numLegislatura=55

        '''
        body = json.loads(response.body_as_unicode())
        data = body['dados']
        links = body['links']
        registration_uri = response.meta['resource_uri']

        _info = {}
        _info['slp:old_resource_uri'] = registration_uri
        _info['slp:resource_uri'] = str(uuid4())

        for attr in data:
            if attr not in IGNORE_TAGS:
                key = 'cam2:{:}'.format(attr)
                _info[key] = aux.text_format(data[attr])

        yield _info