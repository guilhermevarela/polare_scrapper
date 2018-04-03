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
        scrapy runspider spider_memberships_with_role_deputado.py

    Scrapy run + store: 
        scrapy runspider spider_memberships_with_role_deputado.py -o datasets/memberships_with_role_deputado-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
'''
import scrapy
import pandas as pd
import xml.etree.ElementTree as ET

import re

# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX = 'http://www.seliganapolitica.org/resource/'
URL_OPEN_DATA_CAMARA_API_V1 = 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'


IGNORE_TAGS = set([])


class MembershipWithRoleDeputadoSpider(scrapy.Spider):
    name = 'membership_with_role_deputado'


    # Overwrites default:ASCII
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    def __init__(self, legislatura=55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        
        self.congressmen_d = get_congressmen(legislatura)
        self.role    = get_role()
        self.legislatura = 55
        self.prefix = 'cam'

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
                self.parse_membership,
                headers={'accept': 'application/json'},
                meta={'org:hasMember': resource_uri,
                      'resource_idx': resource_idx,
                      'org:role': get_role()}
            )

            yield req

    def parse_membership(self, response):
        '''
            Paginates congressman xml

            ref
                http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=160518&numLegislatura=55

        '''
        root = ET.fromstring(response.body_as_unicode())
        orgdict = {key: response.meta[key]
                   for key in response.meta if 'org:' in key}

        for deputados in root:
            for memberships in deputados.find('./periodosExercicio'):
                result = dict(orgdict)
                result['slp:resource_uri'] = str(uuid4())
                for attr in memberships:
                    if attr.tag not in IGNORE_TAGS:
                        key = '{:}:{:}'.format(self.prefix, attr.tag)
                        value = aux.parse_fn(attr.text)
                        if (len(value) > 0):
                            if 'data' in attr.tag:
                                result[key] = date_format(value)
                            else:
                                result[key] = text_format(attr.text)
                        else:
                            result[key] = None
                yield result


def get_congressmen(legislatura):
    file_path = 'datasets/deputados-{:}.csv'.format(legislatura)
    df = pd.read_csv(file_path, sep=';', index_col=None)
    df = df.set_index('cam:ideCadastro')
    df = df['slp:resource_uri']
    return df.to_frame().to_dict()['slp:resource_uri']


def get_role():
    # Deputado
    return 'b27beba7-ca02-4041-a9e0-1793bcd141fe'


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
    return re.sub(r'  ', ' ', txt)
