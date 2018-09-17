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
        scrapy runspider spider_congressman_with_memberships.py -o datasets/camara/congressman_with_memberships-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
        2018-04-04 updated to query current list of congress on API V2.
        2018-08-30 making it into a standing alone script.
'''
import datetime
import json
import xml.etree.ElementTree as ET

import scrapy
import networkx as nx
# import re


import pandas as pd
import aux

# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX = 'http://www.seliganapolitica.org/resource/'

URL_OPEN_DATA_CAMARA_API_V1 = 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'
URL_OPEN_DATA_CAMARA_API_V2 = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

# FILTER_TAGS_CONGRESSMAN = set(['numLegislatura', 'email', 'data'])
IGNORE_TAGS_TERMS = set(['idCadastroParlamentarAnterior'])
IGNORE_TAGS_AFFILIATIONS = set([])


class CamaraMembershipSpider(scrapy.Spider):
    name = 'camara_memberships'


    # Overwrites default: ASCII
    custom_settings={
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    congressman_with_affiliation_membership = set([
        'idPartidoAnterior', 'siglaPartidoAnterior',
        'nomePartidoAnterior', 'idPartidoPosterior',
        'siglaPartidoPosterior', 'nomePartidoPosterior',
        'dataFiliacaoPartidoPosterior'])

    congressman_with_term_membership = set(
        ['siglaUFRepresentacao', 'descricaoCausaFimExercicio', 'dataInicio','dataFim']
    )

    congressman_mapping = set([
        'ideCadastro', 'nomeCivil', 'nomeParlamentarAtual',
        'dataNascimento', 'dataFalecimento'])

    def __init__(self, *args, **kwargs):
        super(scrapy.Spider).__init__(*args, **kwargs)
        # Parses person json
        self.agents_dict = get_agents()
	    # yml or json?
        self.parties = get_party()

        # Roles dictionary
        self.roles = get_role()
        self.legislatura = legislatura
        self.prefix = 'cam'

        # Process legislatura -- turn into a data interval
        if 'legislatura' in kwargs:
            legislatura = int(kwargs['legislatura'])
        	# 55 --> 2015-02-01, 54 --> 2011-02-01, 53 --> 2007-02-01
        	# legislatura beginings
            start_date = busday_orafter(start_date)
            self.start_date = start_date
        elif 'start_date' in kwargs:
            self.start_date = kwargs['start_date']
        else:
            raise ValueError('Either `legislatura` or `start_date` must be provided') 

        if 'finish_date' in kwargs:
            self.finish_date = kwargs['finish_date']
        else:
            self.finish_date = self.start_date + datetime.timedelta(days=1)

    def start_requests(self):
        '''
            Stage 1: Request Get each congressmen for current term
        '''
        start_str = self.start_date.strftime('%Y %m %d')
        finish_str = self.finish_date.strftime('%Y %m %d')

        url = URL_OPEN_DATA_CAMARA_API_V2
        url = '{:}?idLegislatura={:}'.format(url, self.legislatura)
        url = '{:}&dataInicio={:}'.format(url, start_str)
        url = '{:}&dataFim={:}'.format(url, finish_str)
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
            # resource_uri = self.agents_dict.get(resource_idx, str(uuid4()))

            # req = scrapy.Request(
            #     url,
            #     self.parse_congressman,
            #     headers={'accept': 'application/json'},
            #     meta={'resource_uri': resource_uri,
            #           'resource_idx': resource_idx}
            # )
            req = scrapy.Request(
                url,
                self.parse_congressman,
                headers={'accept': 'application/json'},
                meta={'resource_idx': resource_idx}
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

        # resource_uri = response.meta['resource_uri']
        outputs = {}
        for deputados in root:
            # outputs = {'slnp:resource_uri': resource_uri}
            outputs = {}
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

            outputs['resource_uri'] = self.search_agents_uri(outputs)
            if not outputs['resource_uri']:
                outputs['resource_uri'] = str(uuid4())

            terms = []
            for term in deputados.find('./periodosExercicio'):
                result = {
                    'slnp:resource_uri': str(uuid4()),
                    'org:role': self.roles['Deputy']
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
                    'slnp:resource_uri': str(uuid4()),
                    'org:role': self.roles['Affiliate']
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
                        'slnp:resource_uri': str(uuid4()),
                        'cam:sigla': result['cam:siglaPartidoAnterior'],
                        'cam:startDate': None,
                        'cam:finishDate': result['cam:dataFiliacaoPartidoPosterior'],
                        'org:role': self.roles['Affiliate'],
                    }
                    affiliations.append(affiliation_0)

                affiliation_0 = {
                    'slnp:resource_uri': str(uuid4()),
                    'cam:sigla': result['cam:siglaPartidoPosterior'],
                    'cam:startDate': result['cam:dataFiliacaoPartidoPosterior'],
                    'cam:finishDate': None,
                    'org:role': self.roles['Affiliate']
                }
                affiliations.append(affiliation_0)

            if affiliations:
                outputs['affiliations'] = affiliations
            else:
                id_partido = deputados.find('./partidoAtual/idPartido')
                outputs['affiliations'] = [{
                    'slnp:resource_uri': str(uuid4()),
                    'cam:sigla': id_partido.text,
                    'cam:startDate': None,
                    'cam:finishDate': None,
                    'org:role': self.roles['Affiliate'],
                }]

        yield outputs

    def search_agents_uri(self, outputs):
        mapping_dict = {
            'cam:ideCadastro': 'cam:ideCadastro',
            'cam:nomeCivil': 'cam:nomeCivil',
            'cam:nomeCivil': 'sen:NomeCompletoParlamentar'
        }
        for col_congress, col_agents in mapping_dict.items():
            # lookup_dict = self.agents_dict[col_agents]
            for resource_uri_, resource_id_ in lookup_dict.items():
                if outputs[col_congress] == str(resource_id_):
                    return resource_uri_
        return None


def get_agents(identities_path='identities.json'):
    with open(identities_path, mode='r') as f:
        agents_json = json.load(f)

    if agents_json['info']['type'] != 'PersonIdentity':
        ValueError('identitiesJSON must be of info#type = `PersonIdentity`')

    agents_list = agents_json['data']['person']
    properties_list = agents_json['data']['property']

    agents_graph = nx.DiGraph()
    for a in agents_list:
        source_list = []
        target_list = []
        for p in properties_list:
            pid = p['property_id']
            pvl = p['value']
            pst = '{:}:{:}'.format(pid, pvl)

            if pid == 'seliga_uri':
                source_list.append(pst)
            else:
                target_list.append(pst)

        agents_graph.add_edges_from([
            (src, tgt)
            for src in source_list
            for tgt in target_list
        ])
    return agents_graph


def get_roles():
    roles_path = 'datasets/snlp/roles.csv'
    df = pd.read_csv(roles_path, sep=';', index_col=0)
    roles_dict = df.to_dict()
    for label, d in roles_dict.items():
        roles_dict[label] = {v: k for k, v in d}
    return roles_dict


def get_parties():
    parties_path = 'datasets/snlp/parties.csv'
    df = pd.read_csv(parties_path, sep=';', index_col=0)
    parties_dict = df.to_dict()
    for label, d in parties_dict.items():
        parties_dict[label] = {v: k for k, v in d}
    return parties_dict


def busday_orafter(start_date):
    '''Returns closest business date from start date

    Arguments:
        start_date {datetime.date} -- inital date

    Returns:
        busdate {datetime.date} -- busdate
    '''
    finish_date = start_date + time.delta(21)
    for busdate in daterange(start_date, finish_date):
        return busdate
    return None

def busdays_range(start_date, end_date):
    from dateutil.rrule import DAILY, rrule, MO, TU, WE, TH, FR

    return rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO,TU,WE,TH,FR))
