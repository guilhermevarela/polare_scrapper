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

    Scrapy run + store: scrapy runspider spider_congressman_with_memberships.py -o datasets/camara/congressman_with_memberships-55.json  -a legislatura=55

    updates:
        2018-03-08 updated to use XPaths
        2018-04-04 updated to query current list of congress on API V2.
'''
# from datetime import datetime
# from datetime import date 
import scrapy
import re
import xml.etree.ElementTree as ET 

# import pandas as pd
import json
from aux import get_congressmen, get_party, get_role
#resource_uri generation and testing
# from resource_uri.getters import get_congressmen, get_party
# from resource_uri.setters import set_person_resource_uri

# Unique id without Network address
from uuid import uuid4

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'

URL_OPEN_DATA_CAMARA_API_V1= 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'
URL_OPEN_DATA_CAMARA_API_V2 = 'https://dadosabertos.camara.leg.br/api/v2/deputados'



class CongressmanWithMembershipsSpider(scrapy.Spider):
    name= 'congressman_with_memberships'


    # Overwrites default: ASCII
    custom_settings={
        'FEED_EXPORT_ENCODING': 'utf-8' 
    }

    congressman_with_affiliation_membership = {
        'idPartidoPosterior': 'cam:sigla',
        'dataFiliacaoPartidoPosterior': 'cam:startDate',
    }
    congressman_with_term_membership = {
        'siglaUFRepresentacao': 'cam:siglaUFRepresentacao',
        'dataInicio': 'cam:startDate',
        'dataFim': 'cam:finishDate',
    }

    congressman_mapping={
        'ideCadastro': 'cam:ideCadastro',
        'nomeCivil': 'cam:nomeCivil',
        'nomeParlamentarAtual': 'cam:nomeParlamentarAtual',
        'terms': [],
        'affiliations': [],
    }
    def __init__(self, legislatura= 55, *args,**kwargs):
        super(scrapy.Spider).__init__(*args,**kwargs)
        self.old_congressmen = get_congressmen(deprecated=True)
        self.congressmen = get_congressmen()
        self.parties = get_party()
    
        # Roles dictionary
        self.roles = get_role()
        self.legislatura = legislatura

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
            url = congressman['uri']
            resource_uri = self.congressmen.get(resource_idx, None)

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

    # def parse_membership(self, response):
    def parse_congressman(self, response):
        '''
            Paginates congressman xml

            ref
                http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=160518&numLegislatura=55
        '''
        root = ET.fromstring(response.body_as_unicode())
        orgdict = {key: response.meta[key]
                   for key in response.meta if 'org:' in key}

        for deputados in root:
            import code; code.interact(local=dict(globals(), **locals()))
            for memberships in deputados.find('./periodosExercicio'):
                result = dict(orgdict)
                result['slp:resource_uri'] = str(uuid4())
                for attr in memberships:
                    if attr.tag not in IGNORE_TAGS:
                        key = '{:}:{:}'.format(self.prefix, attr.tag)
                        value = aux.parse_fn(attr.text)
                        if (len(value) > 0):
                            if 'data' in attr.tag:
                                result[key] = aux.date_format(value)
                            else:
                                result[key] = aux.text_format(attr.text)
                        else:
                            result[key] = None
                yield result               

    # def parse_congressmen(self, response): 
    #     '''
    #         INPUT
    #             response: stage 1 response gets each congressmen for current term

    #         OUTPUT
    #             req:            stage 2 for each congressman request the details
            
    #     '''                 
    #     root = ET.fromstring(response.body_as_unicode()) 
        
    #     for congressman_listing in root: 
    #         for item in congressman_listing:
    #             if item.tag == 'ideCadastro':
    #                 registration_id= item.text                  
    #                 req = scrapy.Request(congressman_api_v1_uri(registration_id, self.legislatura), 
    #                     self.parse_congressman, 
    #                     headers= {'accept': 'application/json'}, 
    #                     meta={'registration_id': registration_id}               
    #                 )
    #                 yield req 
    #                 break
            

    # def parse_congressman(self, response):
    #     '''
    #         INPUT
    #             response: stage 2 response gets the details for each congressman

    #         OUTPUT
    #             file 
            
    #     '''                 

    #     root = ET.fromstring(response.body_as_unicode()) 

    #     registration_id = int(response.meta['resource_idx'])
    #     resource_uri = self.congressmen.get(registration_id, str(uuid4()))
        
    #     info={}
    #     info['skos:prefLabel'] = str(registration_id)
    #     info['affiliations'] = []
    #     info['terms'] = []
        
    #     for i, congressman_elem in enumerate(root.findall('./Deputado')):
    #         # import code; code.interact(local=dict(globals(), **locals()))
    #         #Person info
    #         if not('foaf:name' in info or 'rdfs:label' in info or 'resource_uri' in info):
    #             info['foaf:name']= str(congressman_elem.find('./nomeCivil').text)
    #             info['rdfs:label']= str(congressman_elem.find('./nomeParlamentarAtual').text)
    #             info['slp:resource_uri']= resource_uri
            
    #         legislatura= congressman_elem.find('./numLegislatura').text
    #         #Person Memberships:affiliations            
    #         keys=self.congressman_with_affiliation_membership.values()
    #         current_affiliation=dict(zip(
    #             keys, [None]*len(keys))
    #         )
    #         current_affiliation['sigla']= str(congressman_elem.find('./partidoAtual/sigla').text)
    #         current_affiliation['party_resource_uri']= self.parties[current_affiliation['sigla']]
    #         current_affiliation['role_resource_uri']= self.roles['Afiliado']
    #         current_affiliation['membership_resource_uri']= str(uuid4())


    #         # if i==1: 
    #         #     import code; code.interact(local=dict(globals(), **locals()))
    #         info['affiliations'].append(current_affiliation)                        
    #         for affiliation_elem in congressman_elem.findall('./filiacoesPartidarias/'):
    #             affiliation={}
    #             for tag, key in self.congressman_with_affiliation_membership.items(): 
    #                 affiliation[key]= affiliation_elem.find('./{:}'.format(tag)).text # finds all keys

    #                 #customizations  add party uri
    #                 if key in ['sigla']: 
    #                     affiliation['party_resource_uri'] = self.parties[affiliation[key]]

    #                 if re.search('Date', key):
    #                     affiliation[key] = formatter_date(affiliation[key])


    #             if info['affiliations']:        # has previous affiliation
    #                 affiliation['finishDate']=info['affiliations'][-1]['startDate'] 

    #             # adds membership_resource_uri
    #             affiliation['membership_resource_uri']=str(uuid4())
    #             affiliation['role_resource_uri']= self.roles['Afiliado']
                
    #             if current_affiliation:
    #                 if affiliation['sigla']==current_affiliation['sigla']: # update only the startDate
    #                     info['affiliations'][-1]['startDate']= affiliation['startDate']
    #                     current_affiliation=None        
    #             else:
    #                 current_affiliation=None        
    #                 info['affiliations'].append(affiliation)


            
    #         for term_elem in congressman_elem.findall('./periodosExercicio/'):              
    #             term={}
    #             term['legislatura']= legislatura
    #             for tag, key in self.congressman_with_term_membership.items(): 
    #                 term[key]= formatter(term_elem.find('./{:}'.format(tag)).text) # finds all keys

    #                 if re.search('Date', key): 
    #                     if len(term[key])>0:
    #                         term[key] = formatter_date(term[key])
    #                     else:
    #                         term[key] = None

    #             # adds membership_resource_uri
    #             term['membership_resource_uri']=str(uuid4())
    #             term['role_resource_uri']= self.roles['Deputado']
                
    #             info['terms'].append(term)


    #         yield info
                

def formatter(rawtext):
    '''
        Removes malformed characters
    '''
    return re.sub(r' ','', re.sub(r'\n','',rawtext))


def formatter_date(this_date):
    if isinstance(this_date, str):
        return this_date[-4:] + '-' + this_date[3:5] + '-' + this_date[0:2]
    else:
        return this_date.strftime('%Y-%m-%d')


def congressman_api_v1_uri(registration_id, legislatura_id=55):
    uri= URL_OPEN_DATA_CAMARA_API_V1
    uri+='ObterDetalhesDeputado?ideCadastro={:}'.format(registration_id)
    uri+='&numLegislatura={:}'.format(legislatura_id)
    return uri