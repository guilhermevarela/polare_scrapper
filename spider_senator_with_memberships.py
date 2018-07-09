# -*- coding: utf-8 -*-
'''
    Date: Dec 5th, 2017

    Author: Guilherme Varela

    ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

    Three phase strategy:
    1. Get each senator for current term
    2. For each senator get details
    3. Within details gets current membership and membership History

    
    Scrapy shell: 
        1. scrapy shell 'http://legis.senado.leg.br/dadosabertos/senator/lista/legislatura/55'

    Scrapy running: scrapy runspider spider_senator_with_memberships.py

    Scrapy run + store: scrapy runspider spider_senator_with_memberships.py -o datasets/senado/senator_with_memberships-55.json -a legislatura=55
'''
from datetime import datetime
from datetime import date
import scrapy
import re
import xml.etree.ElementTree as ET

#import because of files
import pandas as pd 
import numpy as np 


# Unique id without Network address
from uuid import uuid4 

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'
# URL_OPEN_DATA_SENADO_API_V1= 'http://legis.senado.leg.br/dadosabertos/senator/lista/legislatura/'
URL_OPEN_DATA_SENADO_API_V1 = 'http://legis.senado.leg.br/dadosabertos/senador/'

SENATOR_URI = 'd57a29ff-c69a-4b32-b98a-3dd8f204c0a3'
SENADO_URI = '81311052-e5b6-46fe-87ba-83865fa0ffb0'
AFFILIATE_URI = '6a688541-b16a-45ca-8aa9-fa700373279f'


class SenatorWithMembershipsSpider(scrapy.Spider):
    name = 'senator_with_memberships'


    # Overwrites default: ASCII
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8'
    }

    senator_with_affiliation_membership = {
        'SiglaPartido': 'sigla',
        'DataFiliacao': 'startDate',
        'DataDesfiliacao': 'finishDate',
    }
    senator_elected = {
        'CodigoMandato': 'skos:prefLabel',
        'UfParlamentar': 'natureza',
        'NumeroLegislatura': 'legislatura'
    }
    senator_effective = {
        'DataInicio': 'startDate',
        'DataFim': 'finishDate'
    }
    senator_dependencies = {
        'DescricaoParticipacao': 'description',
        'CodigoParlamentar': 'skos:prefLabel',
        'NomeParlamentar': 'rdfs:label'
    }
    senator_mapping = {
        'CodigoParlamentar': 'skos:prefLabel',
        'NomeCompletoParlamentar': 'foaf:name',
        'NomeParlamentar': 'rdfs:label',
        'terms': [],
        'affiliations': [],
    }
    meta_tags = ['depth', 'download_timeout',
                 'download_slot', 'download_latency']

    def __init__(self, legislatura=55, *args, **kwargs):
        super(scrapy.Spider).__init__(*args, **kwargs)
        # _url = '{:}lista/legislatura/{:}?exercicio=s' # only those which are exercising
        _url = '{:}lista/legislatura/{:}' # only those which are exercising
        _url = _url.format(URL_OPEN_DATA_SENADO_API_V1, legislatura)

        self.legislatura = legislatura
        self.start_urls = [_url]
        self.agents = _getagents()

    def start_requests(self):
        '''
           Requests all senators 
           Who effectively took office
        '''
        url = self.start_urls[0]
        req = scrapy.Request(url, self.parse_senators,
                             headers={'accept': 'application/xml'})
        yield req

    def parse_senators(self, response):
        '''
            Parses information regarding Senator and Office terms and fills 
            senator_mapping (info about foaf:Person) and senator terms 
            (info about membership to senator role)

            Only senators who took office - have membership
            Only senators whose first term is requested are seletected

            args
                self            .:  refence to the current object  
                response        .:  a xml reponse for senators data

            returns
                req             .: queries  <BASE_URL>/CodigoParlamentar/afiliacoes/
        '''

        xpath_senators = './Parlamentares/Parlamentar'
        xpath_id = './IdentificacaoParlamentar/'

        root = ET.fromstring(response.body_as_unicode())

        elem_senators = root.findall(xpath_senators)  # XPath element
        for elem_senator in elem_senators:
            info = {}
            for descriptors_elem in elem_senator.findall(xpath_id):
                if descriptors_elem.tag in self.senator_mapping:
                    key = self.senator_mapping[descriptors_elem.tag]
                    info[key] = descriptors_elem.text

            resource_uri = self._getagent(info['foaf:name'])
            if resource_uri is None:
                resource_uri = str(uuid4())
            info['resource_uri'] = resource_uri


            info['terms'] = []
            for mandates in elem_senator.findall('./Mandatos/'):

                _area = mandates.find('UfParlamentar').text
                _code = mandates.find('CodigoMandato').text
                _description = mandates.find('DescricaoParticipacao').text


                for effecterms in mandates.findall('./Exercicios/'):
                    _term = {'area': _area,
                             'code': _code,
                             'description': _description}
                    for effecterm in effecterms:
                        if effecterm.tag in self.senator_effective:
                            _key = self.senator_effective[effecterm.tag]
                            _term[_key] = effecterm.text
                    _term['resource_uri'] = str(uuid4())
                    _term['role'] = SENATOR_URI
                    info['terms'].append(_term)

                self._process_dependents(mandates, info)

            url = senator_api_v1_uri(info['skos:prefLabel'])
            req = scrapy.Request(url,
                self.parse_senator_affiliations,
                headers={'accept': 'application/xml'},
                meta=info
            )
            yield req


    def parse_senator_affiliations(self, response):
        '''
            Parses information regarding Senator and Party affiliations
            senator_mapping (info about foaf:Person) and senator terms 
            (info about membership to senator role)

            args
                self            .:  refence to the current object  
                response  .:  a xml reponse for senators data

            returns
                req        .: queries  <BASE_URL>/CodigoParlamentar/afiliacoes/             
 
        '''
        info = response.meta
        for meta in self.meta_tags:
            del info[meta]

        root = ET.fromstring(response.body_as_unicode())
        affiliations_elem = root.findall('./Parlamentar/Filiacoes/Filiacao') # XPath element
        info['affiliations'] = []
        for affiliation_elem in affiliations_elem:
            affiliation = {}
            for item_elem in affiliation_elem:
                if item_elem.tag == 'Partido':
                    for subitem in item_elem:
                        if subitem.tag in self.senator_with_affiliation_membership:
                            key = self.senator_with_affiliation_membership[subitem.tag]
                            affiliation[key] = subitem.text

                if item_elem.tag in self.senator_with_affiliation_membership:
                    key = self.senator_with_affiliation_membership[item_elem.tag]
                    affiliation[key]= item_elem.text
            affiliation['resource_uri']= str(uuid4())
            affiliation['role'] = AFFILIATE_URI
            info['affiliations'].append(affiliation)

        yield info

    def _process_dependents(self, mandate_root, info):
        '''
            Senator might be Titular, 1rst Suplente 2nd Suplente ...
            parses a senator within url dadosabertos

            mandate_root .:
            info .:
        '''
        info['owner'] = []
        for elected in mandate_root.findall('./Titular'):
            _father = {}
            _father['description'] = elected.find('DescricaoParticipacao').text
            _father['skos:prefLabel'] = elected.find('CodigoParlamentar').text
            _father['rdfs:label'] = elected.find('NomeParlamentar').text
            info['owner'].append(_father)

        info['dependents'] = []
        for dep in mandate_root.findall('./Suplentes/Suplente'):
            _dependents = {}
            _dependents['description'] = dep.find('DescricaoParticipacao').text
            _dependents['skos:prefLabel'] = dep.find('CodigoParlamentar').text
            _dependents['rdfs:label'] = dep.find('NomeParlamentar').text
            info['dependents'].append(_dependents)

        return info


    def _getagent(self, fullname):
        return self.agents['sen:NomeCompletoParlamentar'].get(fullname, None)


def senator_api_v1_uri(person_registration_id):
    uri = URL_OPEN_DATA_SENADO_API_V1
    uri = '{:}{:}/filiacoes'.format(uri, person_registration_id)
    return uri

def _getagents():
    df = pd.read_csv('datasets/slnp/agents.csv', encoding='utf-8', sep= ';', index_col=0)    
    return df.to_dict()