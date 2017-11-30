# -*- coding: utf-8 -*-
'''
	Date: Nov 30th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Three phase strategy:
	1. Get each congressmen for current term
	2. For each congressmen get details
	3. Within details gets current membership and membershipHistory

	
	Scrapy shell: 
		1. scrapy shell 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/ObterDeputados'

	Scrapy running: scrapy runspider spider_congressmen_2_party_v1.py

	Scrapy run + store: scrapy runspider  spider_congressmen_2_party_v1.py -o congressman_membership.json
'''
import scrapy
import re
import xml.etree.ElementTree as ET 

URL_OPEN_DATA_CAMARA_API_V1= 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'



class PoliticalPartyMembershipSpider(scrapy.Spider):
	name= 'political_party_membership'


	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	congressman_mapping={
		'dataNascimento': 'birth_date',
		'idPartidoAnterior':  'previous_party_id',
		'idPartidoPosterior': 'posterior_party_id',
		'dataFiliacaoPartidoPosterior':  'posterior_party_affiliation_date',		
	}
	def start_requests(self): 
		'''
			Stage 1: Request Get each congressmen for current term
		'''
		url = URL_OPEN_DATA_CAMARA_API_V1
		url += 'ObterDeputados'
		req = scrapy.Request(url, 
			self.parse_congressmen, 
			headers= {'accept': 'application/json'}
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

		self.congressmen=[]
		for congressman_listing in root: 
			congressman={}
			for item in congressman_listing:
				if item.tag == 'ideCadastro':
					congressman['registration_id']= item.text 
				if item.tag == 'nome':
					congressman['name']= item.text 					
				if item.tag == 'nomeParlamentar':
					congressman['congressman_name']= item.text 					
					self.congressmen.append( congressman )


					req = scrapy.Request(congressman_uri(congressman['registration_id']), 
						self.parse_congressman_details, 
						headers= {'accept': 'application/json'}, 
						meta={'registration_id':congressman['registration_id'],
							'name':congressman['name'],
							'congressman_name':congressman['congressman_name'],
						 }				
					)
					yield req 
					break
			

	def parse_congressman_details(self, response):
		'''
			INPUT
				response: stage 2 response gets the details for each congressman

			OUTPUT
				file 
			
		'''					

		root = ET.fromstring(response.body_as_unicode()) 
		# import code; code.interact(local=dict(globals(), **locals()))		
		info={key:value for key, value in response.meta.items() if not(('download' in key) | ('depth' in key))}
		
		target_fields= set(self.congressman_mapping.values())
		self.congressmen=[]
		for congressman_details in root: 
			congressman={}
			for item in congressman_details:
				if item.tag in self.congressman_mapping:
					key=self.congressman_mapping[item.tag]
					congressman[key]=formatter(item.text) 

				if item.tag == 'filiacoesPartidarias':					

					for subitem in item: #filiacaoPartidaria
						# import code; code.interact(local=dict(globals(), **locals()))			
						tags=[]
						for subsubitem in subitem:
							if subsubitem.tag in self.congressman_mapping:
								key=self.congressman_mapping[subsubitem.tag]
								tags.append(key)
								congressman[key]=formatter(subsubitem.text) 						
							
							stop= (target_fields == set(congressman.keys()))
							if stop:								
								result=info.copy()
								result.update(congressman) 
								yield result
								for tag in tags:
									del congressman[tag]
								tags=[]	



def formatter(rawtext):
	'''
		Removes malformed characters
	'''
	return re.sub(r'[^A-Za-z0-9|\/]','',rawtext) 



def congressman_uri(registration_id, legislatura_id=''):
	uri= URL_OPEN_DATA_CAMARA_API_V1
	uri+='ObterDetalhesDeputado?ideCadastro=' + registration_id
	uri+='&numLegislatura=' + legislatura_id
	return uri