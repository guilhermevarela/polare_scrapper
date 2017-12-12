# -*- coding: utf-8 -*-
'''
	Date: Nov 14th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	3 step procedure:
	1) From tipo, numero, ano find the proposition id
	2) using the proposition id - fetch all vote events
		 filter only the vote event that is relevant (there might be more than one candidate)
	3) With voteevent_id fetch the votes from each congressman



	proposition:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/proposicoes?siglaTipo=PEC&ano=1993&numero=171&dataInicio=1993-01-01&dataFim=2017-11-17'

	voteevents by preps:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493/votacoes" -H  "accept: application/json"	
	voteevent details: (*INTERESTING: PARTY AFFILIATIONS*)
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/votacoes/6517'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/votacoes/6517" -H  "accept: application/json"
	votes:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/votacoes/6516/votos?itens=513'
	curl -X GET "https://dadosabertos.camara.leg.br/api/v2/votacoes/6516/votos?itens=513" -H  "accept: application/xml"


	Scrapy running: scrapy runspider spider_voteevents.py

	Scrapy run + store: scrapy runspider  spider_voteevents.py -o voteevents.json
	run+store: scrapy runspider spider_voteevents.py  -a tipo='PEC' -a numero='171' -a ano='1993' -o PEC17193.json
'''
from datetime import datetime 
import scrapy
import json 

#https://dadosabertos.camara.leg.br/swagger/api.html
URL_OPEN_DATA_CAMARA= 'https://dadosabertos.camara.leg.br/api/v2/'

class VoteEventsSpider(scrapy.Spider):
	name= 'vote_events'
	custom_settings={
		'FEED_EXPORT_ENCODING': 'utf-8' 
	}


	def __init__(self, tipo, numero, ano, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)		
		self.p_type=tipo 
		self.p_number=numero
		self.p_year=ano 
		self.filename= tipo + numero + '/' + str(ano)[-2:]		
		#last vote_event title
		self.voteevents_title={"PEC": "PROPOSTA DE EMENDA À CONSTITUIÇÃO - SEGUNDO TURNO"}

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
		url  = URL_OPEN_DATA_CAMARA

		url += "proposicoes?siglaTipo=" + self.p_type 
		url += "&ano=" + str(self.p_year) 
		url += "&numero=" + str(self.p_number) 
		url += "&dataInicio=" + str(self.p_year) + "-01-01"
		url += "&dataFim=" + get_today_string()

		req = scrapy.Request(url, 
			self.parse_proposition, 
			headers= {'accept': 'application/json'}
		)
		yield req

	def parse_proposition(self, response):	
		'''
			proposition data

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
		url  = URL_OPEN_DATA_CAMARA
		url += 'proposicoes/' + str(self.proposition_id) + '/votacoes'
		
		req = scrapy.Request(url, 
			self.parse_voteevents_by_proposition, 
			headers= {'accept': 'application/json'}
		)
		yield req		

	def parse_voteevents_by_proposition(self, response):	
		'''
			Lists all vote_events by proposition 
			GUESS: The definitive one

			INPUT 
				response: scrapy.Response object (vote events data)
				

			OUTPUT 
				request: scrapy.Request object
					request url example: 
					https://dadosabertos.camara.leg.br/api/v2/votacoes/6517/votos?itens=513

			
		'''
		def voteevents_stop(voteevent):
			'''
				Stop true then stops procedure
			'''
			if self.p_type in self.voteevents_title:
				p_title=self.voteevents_title[self.p_type]
				return not(p_title == voteevent['titulo'])
			else:
				return False 

		# import code; code.interact(local=dict(globals(), **locals()))		
		_json=json.loads(response.body_as_unicode())
		self.voteevents_by = _json['dados']		
		if isinstance(self.voteevents_by, dict): 
			voteevent=self.voteevents_by
			if not(voteevents_stop(voteevent)):
				voteevent_id = self.voteevents_by['id']
				voteevent_subject = self.voteevents_by['titulo'] 
				
				url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'						
				# import code; code.interact(local=dict(globals(), **locals()))		
				req = scrapy.Request(url, 
					self.parse_votes, 
					headers= {'accept': 'application/json'}, 				
					meta={'voteevent_id': voteevent_id}
				)			
				yield req 		

		else:		
			for voteevent in self.voteevents_by: 	
				if not(voteevents_stop(voteevent)):
					# import code; code.interact(local=dict(globals(), **locals()))		
					voteevent_id = voteevent['id']
					voteevent_subject = voteevent['titulo'] 
					url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'				
					# import code; code.interact(local=dict(globals(), **locals()))		
					req = scrapy.Request(url, 
						self.parse_votes, 
						headers= {'accept': 'application/json'}, 				
						meta={'voteevent_id': voteevent_id,
						'voteevent_subject':voteevent_subject					
						}
					)				
					yield req 		
		

	def parse_votes(self, response):	
		'''
			All votes from a voteevent
		
			INPUT 
				response: scrapy.Response object (vote for each congressman)
				

			OUTPUT 
				voteevent_id: id from vote event
				vote_value:string; Sim, Não, Obstrução
				vote_deputy_name:string; name from congressman
				vote_deputy_id:id from a congressman

			
		'''
		_json=json.loads(response.body_as_unicode())
		self.vote = _json['dados']						
		result=  {k:v for k,v in response.meta.items() if k in ['voteevent_id', 'voteevent_subject']}
		
		for vote in self.vote: 
			result['voteevent_code'] 	= self.filename
			result['vote_value'] 		  = vote['voto']
			result['vote_deputy_name']= vote['parlamentar']['nome']
			result['vote_deputy_id']  = vote['parlamentar']['id']									
			yield result

	


def get_today_string(): 
	todaystr = str(datetime.today()).split(' ')[0] 
	return todaystr