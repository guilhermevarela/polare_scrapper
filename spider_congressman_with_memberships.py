# -*- coding: utf-8 -*-
'''
    Date: Dec 5th, 2017

    Author: Guilherme Varela

    ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

    Three phase strategy:
    1. Get each congressmen for current term
    2. For each congressmen get details
    3. Within details gets current membership and membershipHistory

    
    Scrapy shell: 
        1. scrapy shell 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDeputados'

    Scrapy running: scrapy runspider spider_congressman_with_memberships.py

    Scrapy run + store: 
        scrapy runspider spider_congressman_with_memberships.py -o datasets/camara/json/congressman_with_memberships-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
        2018-04-04 updated to query current list of congress on API V2.
'''
import scrapy
import re
import xml.etree.ElementTree as ET 

# import pandas as pd
import json
import aux

# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'

URL_OPEN_DATA_CAMARA_API_V1= 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'
URL_OPEN_DATA_CAMARA_API_V2 = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

# FILTER_TAGS_CONGRESSMAN = set(['numLegislatura', 'email', 'data'])
IGNORE_TAGS_TERMS = set(['idCadastroParlamentarAnterior'])
IGNORE_TAGS_AFFILIATIONS = set([])

class CongressmanWithMembershipsSpider(scrapy.Spider):
    name= 'congressman_with_memberships'


    # Overwrites default: ASCII
    custom_settings={
        'FEED_EXPORT_ENCODING': 'utf-8' 
    }

    congressman_with_affiliation_membership = set([
        'idPartidoAnterior', 'siglaPartidoAnterior',
        'nomePartidoAnterior', 'idPartidoPosterior',
        'siglaPartidoPosterior', 'nomePartidoPosterior',
        'dataFiliacaoPartidoPosterior'])

    congressman_with_term_membership = set([
        'siglaUFRepresentacao', 'descricaoCausaFimExercicio',
        'dataInicio','dataFim'])

    congressman_mapping = set([
        'ideCadastro', 'nomeCivil', 'nomeParlamentarAtual',
        'dataNascimento', 'dataFalecimento'])

    def __init__(self, legislatura= 55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        self.old_congressmen = aux.get_congressmen(deprecated=True)
        self.congressmen = aux.get_congressmen()
        self.parties = aux.get_party()

        # Roles dictionary
        self.roles = aux.get_role()
        self.legislatura = legislatura
        self.prefix = 'cam'

    def start_requests(self):
        '''
            Stage 1: Request Get each congressmen for current term
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
            url = URL_OPEN_DATA_CAMARA_API_V1
            url = '{:}ObterDetalhesDeputado?ideCadastro='.format(url)
            url = '{:}{:}'.format(url, resource_idx)
            url = '{:}&numLegislatura={:}'.format(url, self.legislatura)
            resource_uri = self.congressmen.get(resource_idx, str(uuid4()))

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
        root = ET.fromstring(response.body_as_unicode())

        resource_uri = response.meta['resource_uri']
        outputs = {}
        for deputados in root:
            outputs = {'slp:resource_uri': resource_uri}
            for attr in deputados:
                if attr.tag in self.congressman_mapping:
                    key = '{:}:{:}'.format(self.prefix, attr.tag)
                    value = aux.parse_fn(attr.text)
                    if (len(value) > 0):
                        if 'data' in attr.tag:
                            outputs[key] = aux.date_format(value)
                        else:
                            outputs[key] = aux.text_format(attr.text)
                    else:
                        outputs[key] = None
            terms = []
            for term in deputados.find('./periodosExercicio'):
                result = {
                    'slp:resource_uri': str(uuid4()),
                    'org:role': self.roles['Deputado']
                }
                for attr in term:
                    if attr.tag in self.congressman_with_term_membership:
                        key = '{:}:{:}'.format(self.prefix, attr.tag)
                        value = aux.parse_fn(attr.text)
                        if (len(value) > 0):
                            if 'data' in attr.tag:
                                result[key] = aux.date_format(value)
                            else:
                                result[key] = aux.text_format(attr.text)
                        else:
                            result[key] = None
                terms.append(result)
            outputs['terms'] = terms

            affiliations = []
            for affiliation in deputados.find('./filiacoesPartidarias'):
                result = {
                    'slp:resource_uri': str(uuid4()),
                    'org:role': self.roles['Afiliado']
                }
                for attr in affiliation:
                    if attr.tag in self.congressman_with_affiliation_membership:
                        key = '{:}:{:}'.format(self.prefix, attr.tag)
                        value = aux.parse_fn(attr.text)
                        if (len(value) > 0):
                            if 'data' in attr.tag:
                                result[key] = aux.date_format(value)
                            else:
                                result[key] = aux.text_format(attr.text)
                        else:
                            result[key] = None
                if affiliations:
                    affiliations[-1]['cam:finishDate'] = result['cam:dataFiliacaoPartidoPosterior']
                else:
                    affiliation_0 = {
                        'slp:resource_uri': str(uuid4()),
                        'cam:sigla': result['cam:siglaPartidoAnterior'],
                        'cam:startDate': None,
                        'cam:finishDate': result['cam:dataFiliacaoPartidoPosterior'],
                        'org:role': self.roles['Afiliado'],
                    }
                    affiliations.append(affiliation_0)

                affiliation_0 = {
                    'slp:resource_uri': str(uuid4()),
                    'cam:sigla': result['cam:siglaPartidoPosterior'],
                    'cam:startDate': result['cam:dataFiliacaoPartidoPosterior'],
                    'cam:finishDate': None,
                    'org:role': self.roles['Afiliado']
                }
                affiliations.append(affiliation_0)

            if affiliations:
                outputs['affiliations'] = affiliations
            else:
                id_partido = deputados.find('./partidoAtual/idPartido')
                outputs['affiliations'] = [{
                    'slp:resource_uri': str(uuid4()),
                    'cam:sigla': id_partido.text,
                    'cam:startDate': None,
                    'cam:finishDate': None,
                    'org:role': self.roles['Afiliado'],
                }]

        yield outputs