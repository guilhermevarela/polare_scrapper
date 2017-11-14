# -*- coding: utf-8 -*-
'''
	Date: Nov 14th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	fetch all deputies's votes for all vote events for a given proposition

	proposition:
	Scrapy shell: scrapy shell 'https://dadosabertos.camara.leg.br/api/v2/proposicoes/1215809'
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
'''
import scrapy
import json 

# https://dadosabertos.camara.leg.br/swagger/api.html
#https://dadosabertos.camara.leg.br/api/v2/partidos?ordenarPor=sigla
URL_OPEN_DATA_CAMARA= 'https://dadosabertos.camara.leg.br/api/v2/'

class RequestHeaderItem(scrapy.Item): 
	partidoid = scrapy.Field() 	
	sigla=      scrapy.Field() 
	nome=       scrapy.Field() 
	uri=        scrapy.Field() 



class VoteEventsSpider(scrapy.Spider):
	name= 'vote_events'
	
	def __init__(self, legal_act, act_id, *args,**kwargs):
		super(scrapy.Spider).__init__(*args,**kwargs)
		self.legal_act = self.name + '_' + legal_act
		self.act_id=act_id

	def start_requests(self): 		
		url  = URL_OPEN_DATA_CAMARA
		url += 'proposicoes/' + str(self.act_id) + '/votacoes'
		req = scrapy.Request(url, 
			self.parse_voteevents_by_proposition, 
			headers= {'accept': 'application/json'}
		)
		yield req

	def parse_voteevents_by_proposition(self, response):	
		'''
		An item of voteevents_by_proposition

		{
      "id": 6517,
      "uri": "https://dadosabertos.camara.leg.br/api/v2/votacoes/6517",
      "titulo": "PROPOSTA DE EMENDA À CONSTITUIÇÃO - SEGUNDO TURNO",
      "uriEvento": "https://dadosabertos.camara.leg.br/api/v2/eventos/40574",
      "proposicao": {
        "id": 15749,
        "uri": "https://dadosabertos.camara.leg.br/api/v2/proposicoes/15749",
        "siglaTipo": null,
        "idTipo": null,
        "numero": null,
        "ano": null,
        "ementa": "Altera a redação do art. 228 da Constituição Federal (imputabilidade penal do maior de dezesseis anos)."
      },
      "uriProposicaoPrincipal": "https://dadosabertos.camara.leg.br/api/v2/proposicoes/14493",
      "tipoVotacao": "Nominal Eletrônica",
      "aprovada": true,
      "placarSim": 320,
      "placarNao": 152,
      "placarAbstencao": "1"
    }
		'''
		
		_json=json.loads(response.body_as_unicode())
		self.voteevents_by = _json['dados']		
		if isinstance(self.voteevents_by, dict): 
			voteevent_id = self.voteevents_by['id']
			url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'
			parser = lambda x : self.parse_votes(voteevent_id, x)
			req = scrapy.Request(url, 
				parser, 
				headers= {'accept': 'application/json'}, 				
			)			
			yield req 		

		else:
			# case array of dictionaries
			for voteevent in self.voteevents_by: 	
				voteevent_id = voteevent['id']
				url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes/' + str(voteevent_id) + '/votos?itens=513'
				parser = lambda x : self.parse_votes(voteevent_id, x)
				req = scrapy.Request(url, 
					parser, 
					headers= {'accept': 'application/json'}, 				
				)				
				yield req 		
	
	def parse_votes(self, voteevent_id, response):	
		'''
		{
      "voto": "Não",
      "parlamentar": {
        "id": 178864,
        "uri": "https://dadosabertos.camara.leg.br/api/v2/deputados/178864",
        "nome": "ADAIL CARNEIRO",
        "siglaPartido": "PHS",
        "uriPartido": "https://dadosabertos.camara.leg.br/api/v2/partidos/36809",
        "siglaUf": "CE",
        "idLegislatura": 55,
        "urlFoto": "http://www.camara.leg.br/internet/deputado/bandep/178864.jpg"
      }
    }    
		'''
		# import code; code.interact(local=dict(globals(), **locals()))		
		_json=json.loads(response.body_as_unicode())
		self.vote = _json['dados']				
		result=  dict([('voteevent_id', voteevent_id)])
		for vote in self.vote: 
			result['vote_value'] 		  = vote['voto']
			result['vote_deputy_name']= vote['parlamentar']['nome']
			result['vote_deputy_id']  = vote['parlamentar']['id']									
			yield result

