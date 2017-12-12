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
		self.df_congressmen = pd.read_csv('resource_uris/person_resource_uri.csv', sep=' ', index_col=0)		
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

		registration_id=response.meta['registration_id']
		person_resource_uri=self.polare_get_person_uri(registration_id)	

		target_fields= set(self.congressman_mapping.values())
		self.congressmen_memberships=[]
		anchor_date= date(2099,1,1)
		draft_date= anchor_date
		memberships=[]
		for congressman_details in root: 
			congressman={}
			for item in congressman_details:
				if item.tag in self.congressman_mapping:
					key=self.congressman_mapping[item.tag]
					congressman[key]=formatter(item.text) 
					
				if item.tag == 'periodosExercicio': 	
					draft_date=self.parsenode_draft_dates(item)

				if item.tag == 'filiacoesPartidarias':		
				 	memberships=self.parsenode_memberships(item)

				if draft_date < anchor_date and len(memberships)>0: 						
					for i, membership in enumerate(memberships):
						if draft_date < anchor_date and i == 0:
							c_m=congressman_membership(congressman, membership, draft_date,person_resource_uri)
							self.congressmen_memberships.append(c_m)
						c_m=congressman_membership(congressman, membership, person_resource_uri=person_resource_uri)
						self.congressmen_memberships.append(c_m)

					
					draft_date=anchor_date
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
		return draft_date

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

	def polare_get_person_uri(self, registration_id):	
		# import code; code.interact(local=dict(globals(), **locals()))		
		ind= self.df_congressmen['ideCadastro'] == int(registration_id)
		if np.any(ind):			
			person_resource_uri=self.df_congressmen.index[ind][0]

		else:
			person_resource_uri=None 
		return person_resource_uri

def congressman_membership(congressman, membership,draft_date=None, person_resource_uri=None):		
		c_m={}
		#Defines person resource uri
		
		if not(person_resource_uri):
			person_resource_uri=formatter_person_resource_uri(congressman['name'], congressman['birth_date'])	
		c_m['person_resource_uri']=person_resource_uri
		if draft_date:
			affiliation_date=formatter_affiliation_date(draft_date)
			party_id=membership['previous_party_id']
		else:						
			affiliation_date=membership['posterior_party_affiliation_date']	
			affiliation_date=formatter_affiliation_date(affiliation_date)
			party_id=membership['posterior_party_id']
		
		c_m['party_resource_uri']=formatter_party_resource_uri(party_id)
		c_m['affiliation_date']=affiliation_date
		return c_m

def formatter(rawtext):
	'''
		Removes malformed characters
	'''
	return re.sub(r'[^A-Za-z0-9|\/]','',rawtext) 


def formatter_party_resource_uri(partyid):
	return 'object/' + partyid

def formatter_person_resource_uri(person_name, person_birthdate):
	return 'object/' + person_name + person_birthdate

def formatter_affiliation_date(affiliation_date):
	if isinstance(affiliation_date, str):
		return affiliation_date[-4:] + '-' + affiliation_date[3:5] + '-' + affiliation_date[0:2]
	else:
		return affiliation_date.strftime('%Y-%m-%d')


def congressman_api_v1_uri(registration_id, legislatura_id=''):
	uri= URL_OPEN_DATA_CAMARA_API_V1
	uri+='ObterDetalhesDeputado?ideCadastro=' + registration_id
	uri+='&numLegislatura=' + legislatura_id
	return uri