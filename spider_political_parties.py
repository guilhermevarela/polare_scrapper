# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro'

	Scrapy running: scrapy runspider spider_tse_parties.py

	Scrapy run + store: scrapy runspider spider_political_parties.py -o datasets/political_parties_with_president.json
'''
import scrapy
import re 
import pandas as pd

from resource_uri.getters import get_party
from resource_uri.setters import set_party_resource_uri
# Unique id without Network address
from uuid import uuid4 
stopwords=[
	'\n'
]

POLARE_PREFIX='http://www.seliganapolitica.org/resource/'

class TsePoliticalPartiesSpider(scrapy.Spider):
	name= 'tse_political_parties'
	start_urls = ['http://www.tse.jus.br/partidos/partidos-politicos/registrados-no-tse']
	
	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	column_fields={1: 'party_code', 
	2:'party_name', 
	3:'party_founding_date', 	
	4:'party_presidency', 	
	5:'party_id'}

	def __init__(self,  *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)		
		self.db_parties = get_party()

		# Roles dictionary
		d= pd.read_csv('resource_uri/role_resource_uri.csv', sep= ';', index_col=0).to_dict()['skos:prefLabel']
		self.db_roles = {v:k for k,v in d.items()}		

		# Senador uri's
		df= pd.read_csv('resource_uri/senadores_resource_uri-55.csv', sep= ';', index_col=0)		
		d= df['rdfs:label'].to_dict()		
		self.db_senators = {v.upper():k for k,v in d.items()}		
	
	def parse(self, response): 
		# tds = response.xpath('//tbody/tr//td[@class="tabelas"]')
		tds = response.xpath('//tbody/tr//td')
		ncols=6
		this_party= {} 

		for i, td in enumerate(tds):			
			row = int(i / ncols)
			col = i - row*ncols
			
			if col in self.column_fields: 
				values=  td.xpath('.//text()').extract()
				field_name=self.column_fields[col]

				# Due to weird formatting we must grab the first non-empty value				
				value=self.formatter(values, field_name)				
				this_party[field_name]= value

				if field_name == 'party_code':
					this_party['party_resource_uri']= self.db_parties[value]

			if (col == ncols-1):
				this_party['role_resource_uri']= self.db_roles['Presidente']
				yield this_party
				this_party={}

	def formatter(self, values, field_name):				
		'''
			Formats a raw text read from html element. 
			Formats dates to CCYY-MM-DD 
			INPUT:
				values<list(str)>: a list of web texts should have one element only

				field_name<str>: 	a str indicating the field name being read

			OUTPUT:
				result<str> the single string representing the formatted text
		'''	
		values=list(filter(lambda x : not(x in stopwords), values))
		result=values[0] if len(values)>0 else None			
		if not(result==None):
			if field_name=='party_founding_date':			
				regexp_date= re.sub(r'[^0-9|\.]', '', result) 
				if regexp_date==None:
					result=None 
				else:
					tmp= regexp_date.split('.')			
					yyyy= int(tmp[-1])
					m=int(tmp[1])
					d=int(tmp[0])						
				result= '%4d-%02d-%02d' % (yyyy,m,d)

			if 	field_name=='party_presidency':				
				result= result.split(',')[0]
				result= self.db_senators.get(result, str(uuid4()))
		return result





