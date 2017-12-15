# -*- coding: utf-8 -*-
'''
	Date: Oct 09th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Scrapy shell: scrapy shell 'http://www.tse.jus.br/partidos/partidos-politicos/partido-do-movimento-democratico-brasileiro'

	Scrapy running: scrapy runspider spider_tse_parties.py

	Scrapy run + store: scrapy runspider spider_tse_political_parties.py -o tse_political_parties.json
'''
import scrapy
import re 

from resource_uri.getters import get_party_uri_by_code
from resource_uri.setters import set_party_resource_uri
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
	5:'party_id'}

	def __init__(self,  *args,**kwargs):
			super(scrapy.Spider).__init__(*args,**kwargs)		
			self.dbparties = get_party_uri_by_code()
	
	def parse(self, response): 
		# tds = response.xpath('//tbody/tr//td[@class="tabelas"]')
		tds = response.xpath('//tbody/tr//td')
		ncols=6
		this_party= {} 
		# is_ready=False
		for i, td in enumerate(tds):			
			row = int(i / ncols)
			col = i - row*ncols
			
			if col in self.column_fields: 
				values=  td.xpath('.//text()').extract()
				field_name=self.column_fields[col]

				# Due to weird formatting we must grab the first non-empty value				
				value=formatter(values, field_name)				
				this_party[field_name]= value

				if field_name == 'party_code':
					this_party['party_resource_uri']= self.dbparties[value]
				# if not(is_ready):
				# 	if test_is_ready(parties):
				# 		founding_date_array=parties['party_founding_date'].split('-')
			
				# 		parties['party_resource_uri']= 	\
				# 			POLARE_PREFIX + \
				# 			set_party_resource_uri(parties['party_id'], *founding_date_array)					
			
			if (col == ncols-1):
				yield this_party
				this_party={}

# for geenerated ids
# def test_is_ready(parties):
# 	return ('party_founding_date' in parties) and ('party_id' in parties)

def formatter(values, field_name):				
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

	return result





