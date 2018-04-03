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

	Scrapy run + store: scrapy runspider spider_congressman_with_memberships.py -o datasets/congressman_with_memberships-55.json  -a legislatura=55

	updates:
		2018-03-08 updated to use XPaths
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
from resource_uri.getters import get_congressmen, get_party
from resource_uri.setters import set_person_resource_uri

# Unique id without Network address
from uuid import uuid4 

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'
URL_OPEN_DATA_CAMARA_API_V1= 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'



class CongressmenAndPartyMembershipsSpider(scrapy.Spider):
	name= 'congressman_with_memberships'


	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	congressman_with_affiliation_membership = {	
		'idPartidoPosterior': 'sigla',
		'dataFiliacaoPartidoPosterior': 'startDate',		
	}
	congressman_with_term_membership = {	
		'siglaUFRepresentacao': 'natureza',
		'dataInicio': 'startDate',
		'dataFim': 'finishDate',				
	}

	congressman_mapping={
	 	'ideCadastro': 'registration_id',
	 	'nomeCivil': 'foaf:name',
	 	'nomeParlamentarAtual': 'rdfs:label',
	 	'terms': [],
		'affiliations': [],
	}
	def __init__(self, legislatura= 55, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)				
		self.db_congressmen_uri = get_congressmen()
		self.db_party_uri= get_party()
		# self.congressmen={} # use registration id as key
	
		# Roles dictionary
		d= pd.read_csv('resource_uri/role_resource_uri.csv', sep= ';', index_col=0).to_dict()['skos:prefLabel']
		self.db_roles = {v:k for k,v in d.items()}		
		self.legislatura= legislatura

	def start_requests(self): 
		'''
			Stage 1: Request Get each congressmen for current term
		'''
		# url = URL_OPEN_DATA_CAMARA_API_V1
		# url += 'ObterDeputados'
		# req = scrapy.Request(url, 
		# 	self.parse_congressmen, 
		# 	headers= {'accept': 'application/json'}
		# )		
		req = scrapy.Request('http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDetalhesDeputado?ideCadastro=74847&numLegislatura=', 
			self.parse_congressman, 
			headers= {'accept': 'application/json'},
			meta={'registration_id': '74847'}				
		)		

		yield req

	def parse_congressmen(self, response): 
		'''
			INPUT
				response: stage 1 response gets each congressmen for current term

			OUTPUT
				req: 			stage 2 for each congressman request the details
			
		'''					
		root = ET.fromstring(response.body_as_unicode()) 
		
		for congressman_listing in root: 
			for item in congressman_listing:
				if item.tag == 'ideCadastro':
					registration_id= item.text 					
					req = scrapy.Request(congressman_api_v1_uri(registration_id, self.legislatura), 
						self.parse_congressman, 
						headers= {'accept': 'application/json'}, 
						meta={'registration_id': registration_id}				
					)
					yield req 
					break
			

	def parse_congressman(self, response):
		'''
			INPUT
				response: stage 2 response gets the details for each congressman

			OUTPUT
				file 
			
		'''					

		root = ET.fromstring(response.body_as_unicode()) 

		registration_id=int(response.meta['registration_id'])
		agent_resource_uri=None 
		if registration_id in self.db_congressmen_uri:
			agent_resource_uri=self.db_congressmen_uri[registration_id]
		else:
			agent_resource_uri=str(uuid4())

		
		info={}
		info['skos:prefLabel']= str(registration_id)
		info['affiliations']=[]
		info['terms']=[]			
		
		for i, congressman_elem in enumerate(root.findall('./Deputado')):
			import code; code.interact(local=dict(globals(), **locals()))
			#Person info
			if not('foaf:name' in info or 'rdfs:label' in info or 'agent_resource_uri' in info):
				info['foaf:name']= str(congressman_elem.find('./nomeCivil').text)
				info['rdfs:label']= str(congressman_elem.find('./nomeParlamentarAtual').text)
				info['agent_resource_uri']= agent_resource_uri
			
			legislatura= congressman_elem.find('./numLegislatura').text
			#Person Memberships:affiliations			
			keys=self.congressman_with_affiliation_membership.values()
			current_affiliation=dict(zip(
				keys, [None]*len(keys))
			)
			current_affiliation['sigla']= str(congressman_elem.find('./partidoAtual/sigla').text)
			current_affiliation['party_resource_uri']= self.db_party_uri[current_affiliation['sigla']]
			current_affiliation['role_resource_uri']= self.db_roles['Afiliado']
			current_affiliation['membership_resource_uri']= str(uuid4())


			if i==1: 
				import code; code.interact(local=dict(globals(), **locals()))
			info['affiliations'].append(current_affiliation)						
			for affiliation_elem in congressman_elem.findall('./filiacoesPartidarias/'):
				affiliation={}
				for tag, key in self.congressman_with_affiliation_membership.items(): 
					affiliation[key]= affiliation_elem.find('./{:}'.format(tag)).text # finds all keys

					#customizations  add party uri
					if key in ['sigla']: 
						affiliation['party_resource_uri'] = self.db_party_uri[affiliation[key]]

					if re.search('Date', key):
						affiliation[key] = formatter_date(affiliation[key])


				if info['affiliations']:		# has previous affiliation
					affiliation['finishDate']=info['affiliations'][-1]['startDate'] 

				# adds membership_resource_uri
				affiliation['membership_resource_uri']=str(uuid4())
				affiliation['role_resource_uri']= self.db_roles['Afiliado']
				
				if current_affiliation:
					if affiliation['sigla']==current_affiliation['sigla']: # update only the startDate
						info['affiliations'][-1]['startDate']= affiliation['startDate']
						current_affiliation=None		
				else:
					current_affiliation=None		
					info['affiliations'].append(affiliation)


			
			for term_elem in congressman_elem.findall('./periodosExercicio/'):				
				term={}
				term['legislatura']= legislatura
				for tag, key in self.congressman_with_term_membership.items(): 
					term[key]= formatter(term_elem.find('./{:}'.format(tag)).text) # finds all keys

					if re.search('Date', key): 
						if len(term[key])>0:
							term[key] = formatter_date(term[key])
						else:
							term[key] = None

				# adds membership_resource_uri
				term['membership_resource_uri']=str(uuid4())
				term['role_resource_uri']= self.db_roles['Deputado']
				
				info['terms'].append(term)


			yield info
				

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