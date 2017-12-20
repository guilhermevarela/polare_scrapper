# -*- coding: utf-8 -*-
'''
	Date: Dec 15th, 2017

	Author: Guilherme Varela

	Provides uris for Polare Ontology resources 

'''
import hashlib


POLARE_PREFIX='http://www.seliganapolitica.org/resource/'

def set_person_resource_uri(name, year):
	'''
		Generates uri for foaf:Person		

		SEE Periodo 2016.1 (Paralmentares v.1)/Codigo Python para gerar uri de pessoas.txt
		INPUT
			name<string>:

			year<string>: 2-digit string representing the year of birth 

		OUTPUT
			resource_uri<string>: md5
	'''	
	year1=''
	if year !='' and year !='99':
	 year1=year

	splistring = name.split()
	splistring=sorted(splistring)
	resource_id = ''
	for x in splistring:
	   resource_id = resource_id + x
	resource_id = resource_id+','+year1
	resource_id = resource_id.encode('utf-8');
	
	return hashlib.md5(resource_id).hexdigest()


def  set_party_resource_uri(party_number, founding_yyyy, 
																		founding_mm, founding_dd):
	'''
		Generates uri for a org:FormalOrganization

		resource_id -> 'party_number,YYYY,MM,DD'
		INPUT
			party_number<string>:  2-digit string  representing party number

			founding_yyyy<string>: 4-digit string representing year

			founding_mm<string>: 2-digit string representing mouth

			founding_dd<string>: 2-digit string representing day

		OUTPUT
			resource_uri<string>: md5

	'''
	if len(founding_yyyy)!=4:
		raise ValueError('Founding year must be a 4 digit string')
	
	if int(founding_mm)<1 or int(founding_mm)>2099:
		raise ValueError('Founding year invalid')	

	if len(founding_mm)!=2:
		raise ValueError('Founding month must be a 2 digit string')	
	
	if int(founding_mm)<1 or int(founding_mm)>12:
		raise ValueError('Founding month must be between 1 and 12')	

	if len(founding_dd)!=2:
		raise ValueError('Founding day must be a 2 digit string')	

	
	resource_id = '%s,%s,%s,%s' % (party_number, founding_yyyy, founding_mm, founding_dd)
	resource_id =resource_id.encode('utf-8')
	return POLARE_PREFIX + hashlib.md5(resource_id).hexdigest()