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

	Scrapy running: scrapy runspider spider_congressmen_and_party_memberships.py

	Scrapy run + store: scrapy runspider spider_congressmen_and_party_memberships.py -o congressman_and_party_membership.json
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

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'
URL_OPEN_DATA_CAMARA_API_V1= 'http://www.camara.leg.br/SitCamaraWS/Deputados.asmx/'



class CongressmenAndPartyMembershipsSpider(scrapy.Spider):
	name= 'congressmen_and_paty_memberships'


	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	congressman_mapping={
	 	'ideCadastro': 'registration_id',
	 	'nomeCivil': 'name',
	 	'nomeParlamentarAtual': 'congressman_name',
		'dataNascimento': 'birth_date',
		'idPartidoAnterior':  'previous_party_id',
		'idPartidoPosterior': 'posterior_party_id',
		'dataFiliacaoPartidoPosterior':  'posterior_party_affiliation_date',		
	}
	def __init__(self, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)				
		# import code; code.interact(local=dict(globals(), **locals()))		
		self.dbcongressmen = get_congressmen_uri_by_apiid()
		self.congressmen={} # use registration id as key
	

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
		
		for congressman_listing in root: 
			for item in congressman_listing:
				if item.tag == 'ideCadastro':
					registration_id= item.text 
					req = scrapy.Request(congressman_api_v1_uri(registration_id), 
						self.parse_congressman_details, 
						headers= {'accept': 'application/json'}, 
						meta={'registration_id': registration_id}				
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

		registration_id=int(response.meta['registration_id'])
		person_resource_uri=None 
		if registration_id in self.dbcongressmen:
			person_resource_uri=self.dbcongressmen[registration_id]
		# person_resource_uri=self.polare_get_person_uri(registration_id)	
		

		target_fields= set(self.congressman_mapping.values())
		self.congressmen_memberships=[]
		draft_date= formatter_date(date(2099,1,1))
		
		memberships=[]
		for congressman_details in root: 
			congressman={}
			start_date= draft_date	
			for item in congressman_details:
				if item.tag in self.congressman_mapping:
					key=self.congressman_mapping[item.tag]
					congressman[key]=formatter(item.text) 
				

				if item.tag == 'periodosExercicio': 	
					start_date=self.parsenode_draft_dates(item)

				if item.tag == 'filiacoesPartidarias':		
					memberships=self.parsenode_memberships(item)

				if start_date < draft_date and len(memberships)>0: 											
					if not(person_resource_uri):
						person_resource_uri=formatter_person_resource_uri(congressman['name'], congressman['birth_date'])	

					for i, membership in enumerate(memberships):
						if formatter_date(start_date) < formatter_date(draft_date) and i == 0:
							c_m=congressman_membership(membership, start_date, use_previous_party=True) 	
							c_m['person_resource_uri']=person_resource_uri							
							new_name='%s %s' % (congressman['name'][0],congressman['name'][-1])
							c_m['generated_person_resource_uri']=set_person_resource_uri(new_name, congressman['birth_date'][-2:])
							self.congressmen_memberships.append(c_m)
							start_date=c_m['finish_date']

						c_m=congressman_membership(membership, start_date)
						c_m['person_resource_uri']=person_resource_uri
						new_name='%s %s' % (congressman['name'][0],congressman['name'][-1])
						c_m['generated_person_resource_uri']=set_person_resource_uri(new_name, congressman['birth_date'][-2:])
						start_date=c_m['finish_date']
						self.congressmen_memberships.append(c_m)					
					break	
				# start_date=draft_date	
				memberships=[]
		for c_m in self.congressmen_memberships:
			yield c_m 				



	def parsenode_draft_dates(self, root_draftperiods):							
		'''
			INPUT
				root_draftperiods: root node to congress man's drafts

			OUTPUT				
				draft_date
		'''			
		draft_date= date(2099,1,1)
		for subitem in root_draftperiods: #periodosExercicio
			tags=[]
			for subsubitem in subitem:
				if subsubitem.tag == 'dataInicio':
					this_date=datetime.strptime(subsubitem.text, '%d/%m/%Y').date()   
					if this_date<=draft_date:
						draft_date=this_date
		return formatter_date(draft_date)

	def parsenode_memberships(self, root_memberships):
		'''
			INPUT
				root_draftperiods: root node to congress man's drafts

			OUTPUT				
				list<dict<str,str>>: returns a list of memberships
		'''				
		memberships=[]
		stop_fields=set(['previous_party_id','posterior_party_id','posterior_party_affiliation_date'])
		for subitem in root_memberships: #filiacaoPartidaria
			membership={}
			for subsubitem in subitem:
				if subsubitem.tag in self.congressman_mapping:
					key=self.congressman_mapping[subsubitem.tag]
					membership[key]=formatter(subsubitem.text) 						
					
				stop= (stop_fields == set(membership.keys()))
				if stop: 
					memberships.append(membership)
					break

		return memberships	

	# def polare_get_person_uri(self, registration_id):	
	# 	# import code; code.interact(local=dict(globals(), **locals()))		
	# 	ind= self.df_congressmen['ideCadastro'] == int(registration_id)
	# 	if np.any(ind):			
	# 		person_resource_uri=self.df_congressmen.index[ind][0]
	# 	else:
	# 		person_resource_uri=None 
	# 	return person_resource_uri

def congressman_membership(membership, start_date, use_previous_party=False):		
		c_m={}

		# start_date=formatter_date(start_date)	
		if use_previous_party:
			party_code=membership['previous_party_id']
		else:
			party_code=membership['posterior_party_id']

		finish_date=membership['posterior_party_affiliation_date']	
		finish_date=formatter_date(finish_date)
		party_id=membership['posterior_party_id']
		
		c_m['party_resource_uri']=formatter_party_resource_uri(party_code)
		c_m['start_date']=start_date
		c_m['finish_date']=finish_date

		return c_m

def formatter(rawtext):
	'''
		Removes malformed characters
	'''
	return re.sub(r'[^A-Za-z0-9|\/| ]','',rawtext) 


def formatter_party_resource_uri(partyid):
	return 'object/' + partyid

def formatter_person_resource_uri(person_name, person_birthdate):
	return 'object/' + person_name + person_birthdate

def formatter_date(this_date):
	if isinstance(this_date, str):
		return this_date[-4:] + '-' + this_date[3:5] + '-' + this_date[0:2]
	else:
		return this_date.strftime('%Y-%m-%d')


def congressman_api_v1_uri(registration_id, legislatura_id=''):
	uri= URL_OPEN_DATA_CAMARA_API_V1
	uri+='ObterDetalhesDeputado?ideCadastro=' + registration_id
	uri+='&numLegislatura=' + legislatura_id
	return uri