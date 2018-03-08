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

	Scrapy run + store: scrapy runspider spider_senator_with_memberships.py -o datasets/senator_with_memberships-55_56.json
'''
from datetime import datetime
from datetime import date 
import scrapy
import re
import xml.etree.ElementTree as ET 

#import because of files
import pandas as pd 
import numpy as np 

#resource_uri generation and testing
from resource_uri.getters import get_congressmen_uri_by_apiid, get_party_uri_by_code
from resource_uri.setters import set_person_resource_uri

# Unique id without Network address
from uuid import uuid4 

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'
# URL_OPEN_DATA_SENADO_API_V1= 'http://legis.senado.leg.br/dadosabertos/senator/lista/legislatura/'
URL_OPEN_DATA_SENADO_API_V1= 'http://legis.senado.leg.br/dadosabertos/senador/'



class SenatorWithMembershipsSpider(scrapy.Spider):
	name= 'senator_with_memberships'


	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	senator_with_affiliation_membership = {	
		'SiglaPartido': 'sigla',
		'DataFiliacao': 'startDate',
		'DataDesfiliacao': 'finishDate',				
	}
	senator_with_term_membership = {	
		'CodigoMandato': 'rdfs:label',
		'UfParlamentar': 'natureza',
		'NumeroLegislatura': 'legislatura', 
		'DataInicio': 'startDate',
		'DataFim': 'finishDate',				
	}

	senator_mapping={
	 	'CodigoParlamentar': 'skos:prefLabel',
	 	'NomeCompletoParlamentar': 'rdfs:label',
	 	'NomeParlamentar': 'rdfs:seeAlso',
		'terms': [],
		'affiliations': [],
	}
	meta_tags= ['depth', 'download_timeout', 'download_slot', 'download_latency']

	def __init__(self, legislatura= 55, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)				
		self.start_urls = [ 
			'{:}lista/legislatura/{:}'.format(URL_OPEN_DATA_SENADO_API_V1, legislatura)
		]

		# Roles dictionary
		d= pd.read_csv('resource_uri/role_resource_uri.csv', sep= ';', index_col=0).to_dict()['skos:prefLabel']
		self.db_roles = {v:k for k,v in d.items()}		

		# Senador uri's
		df= pd.read_csv('resource_uri/senadores_resource_uri-55.csv', sep= ';', index_col=0)		
		d= df['rdfs:label'].to_dict()		
		self.db_senators = {v:k for k,v in d.items()}		

		

	def start_requests(self): 
		'''
			Stage 1: Request Get each senator for current term
		'''
		url = self.start_urls[0]		
		req = scrapy.Request(url, 
			self.parse_senator,
			headers= {'accept': 'application/xml'}
		)
		yield req

	def parse_senator(self, response): 
		'''
			Parses information regarding Senator and Office terms and fills 
			senator_mapping (info about foaf:Person) and senator terms 
			(info about membership to senator role)

			args
				self 			.:  refence to the current object  
				response  .:  a xml reponse for senators data

			returns
				req        .: queries  <BASE_URL>/CodigoParlamentar/afiliacoes/ 			
			
		'''					

		root = ET.fromstring(response.body_as_unicode()) 				
		parlamentares_elem= root.findall('./Parlamentares/Parlamentar') # XPath element
		for parlamentar_elem in  parlamentares_elem:			
			info={}
			for descriptors_elem in parlamentar_elem.findall('./IdentificacaoParlamentar/'):
				if descriptors_elem.tag in self.senator_mapping:
					key= self.senator_mapping[descriptors_elem.tag]
					info[key]= descriptors_elem.text
			
			info['resource_uri']= self.db_senators[info['rdfs:label']]											

			#fills office terms as senator
			info['terms']=[]
			for terms_elem in parlamentar_elem.findall('./Mandatos/'):
				term={}
				subterm_count=1
				for term_elem in terms_elem:					
					if term_elem.tag in self.senator_with_term_membership:
						key= self.senator_with_term_membership[term_elem.tag]
						term[key]= term_elem.text
					
					#DateStart/ DateFinish
					if re.search('LegislaturaDoMandato', term_elem.tag):							
						subterm={}
						for subterm_elem in term_elem:
							if subterm_elem.tag in self.senator_with_term_membership:
								key= str(self.senator_with_term_membership[subterm_elem.tag])
								subterm[key]= subterm_elem.text												

						subterm.update(term)
						subterm['resource_uri']= str(uuid4())
						subterm['role_resource_uri']= self.db_roles['Senador']
						info['terms'].append(subterm)			

				url= senator_api_v1_uri(info['skos:prefLabel'])			
				req = scrapy.Request(url, 
					self.parse_senator_affiliations,
					headers= {'accept': 'application/xml'},
					meta=info
				)									
				
				yield req


	def parse_senator_affiliations(self, response):	
		'''
			Parses information regarding Senator and Party affiliations
			senator_mapping (info about foaf:Person) and senator terms 
			(info about membership to senator role)

			args
				self 			.:  refence to the current object  
				response  .:  a xml reponse for senators data

			returns
				req        .: queries  <BASE_URL>/CodigoParlamentar/afiliacoes/ 			
			
		'''							
		
		
		info= response.meta 
		for meta in self.meta_tags:		
			del info[meta]

		root = ET.fromstring(response.body_as_unicode()) 			
		affiliations_elem= root.findall('./Parlamentar/Filiacoes/Filiacao') # XPath element
		info['affiliations']= []
		for affiliation_elem in affiliations_elem:
			affiliation={}
			for item_elem in affiliation_elem:				
				if item_elem.tag == 'Partido':
					for subitem in item_elem:
						if subitem.tag in self.senator_with_affiliation_membership:
							key= self.senator_with_affiliation_membership[subitem.tag]
							affiliation[key]= subitem.text
				
				if item_elem.tag in self.senator_with_affiliation_membership:
					key= self.senator_with_affiliation_membership[item_elem.tag]
					affiliation[key]= item_elem.text
			affiliation['resource_uri']= str(uuid4())
			affiliation['role_resource_uri']= self.db_roles['Afiliado']
			info['affiliations'].append(affiliation)

		yield info		

def senator_api_v1_uri(person_registration_id):
	uri= URL_OPEN_DATA_SENADO_API_V1
	uri= '{:}{:}/filiacoes'.format(uri, person_registration_id)
	return uri