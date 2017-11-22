# -*- coding: utf-8 -*-
'''
	Date: Nov 22nd, 2017
	
	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	From tipo, numero, ano instanciate a proposition

	Scrapy: 
		run: 			 scrapy runspider  spider_propositions.py -a tipo='PEC' -a numero='171' -a ano='1993' 
		run+store: scrapy runspider  spider_propositions.py -a tipo='PEC' -a numero='171' -a ano='1993' -o PEC17193.json
'''
from datetime import datetime 
import scrapy
import json 

#https://dadosabertos.camara.leg.br/swagger/api.html
URL_OPEN_DATA_CAMARA_API_V2= 'https://dadosabertos.camara.leg.br/api/v2/'
class VoteEventsSpider(scrapy.Spider):
	name= 'propositions'

	# Overwrites default: ASCII
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}

	def __init__(self, tipo, numero, ano, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)		
		
		self.proposition_type=tipo 
		self.proposition_number=numero
		self.proposition_year=ano 
		self.filename= tipo + numero + '/' + str(ano)[-2:]		


	def start_requests(self): 			
		'''
			Finds the proposition id for further queries

			INPUT command line arguments: 
				tipo: short string token; PEC, MPV, PL
				numero: A number (unique within the year) 
				ano: An year (in YYYY format)

			OUTPUT scrapy request object 

			example: 
			'https://dadosabertos.camara.leg.br/api/v2/proposicoes?siglaTipo=PEC&ano=1993&numero=171&dataInicio=1993-01-01&dataFim=2017-11-17'

		'''
		url  = URL_OPEN_DATA_CAMARA_API_V2

		url += "proposicoes?siglaTipo=" + self.proposition_type
		url += "&ano=" + str(self.proposition_year) 
		url += "&numero=" + str(self.proposition_number) 
		url += "&dataInicio=" + str(self.proposition_year) + "-01-01"
		url += "&dataFim=" + get_today_string()

		req = scrapy.Request(url, 
			self.parse_propositions, 
			headers= {'accept': 'application/json'}
		)
		yield req	

	def parse_propositions(self, response):	
		'''
			Returns all propositions by code

			INPUT 
				response: scrapy.Response object (proposition data)
		
			OUTPUT 
				request: scrapy.Request object
							request url example: 
							https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes'

		'''
		jsonfied =json.loads(response.body_as_unicode())
		# import code; code.interact(local=dict(globals(), **locals()))		

		if isinstance(jsonfied['dados'], list):
			self.proposition= jsonfied['dados'][0]
		else:
			self.proposition= jsonfied['dados']
				

		self.proposition_id = self.proposition['id']
		url  = URL_OPEN_DATA_CAMARA_API_V2
		url += 'proposicoes/' + str(self.proposition_id)
		
		req = scrapy.Request(url, 
			self.parse_proposition_id, 
			headers= {'accept': 'application/json'}
		)
		yield req		

	def parse_proposition_id(self, response):
		'''
			Returns a proposition details

			INPUT 
				response: scrapy.Response object (proposition data)
		
			OUTPUT 
				request: scrapy.Request object
							request url example: 
							https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes'

		'''
		jsonfied =json.loads(response.body_as_unicode())

		yield jsonfied['dados']		


def get_today_string(): 
	todaystr = str(datetime.today()).split(' ')[0] 
	return todaystr
