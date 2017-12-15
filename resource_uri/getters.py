# -*- coding: utf-8 -*-
'''
	Date: Dec 15th, 2017

	Author: Guilherme Varela

	Provides pre existing resources

'''
import pandas as pd 

def get_congressmen_uri_by_apiid():
	'''
		Provides a dictionary with keys being api ids 
		OUTPUT
			dict<key<int>,value<string>>: dictionary
				key: ideCadastro api v1 camara
				value: resource uri
	'''
	result= get_resource('person_resource_uri.csv', 'ideCadastro', 'person_resource_uri')			

	return result

def get_party_uri_by_code():
	'''
		Provides a dictionary with keys being api ids 
		OUTPUT
			dict<key<int>,value<string>>: dictionary
				key: ideCadastro api v1 camara
				value: resource uri
	'''
	result= get_resource('party_resource_uri.csv', 'Sigla', 'party_resource_uri')			
	
	return result

def get_resource(table, key_column, resource_uri_column):		
	'''
		Provides a dictionary with key=key_column and value=resource_uri

		INPUT
		table<string>
		
		key_column<string>: name of the column that wil become the key

		resource_uri_column<string>:a string representing the name of a column 

		OUTPUT
			dict<key<int>,polare_resource_uri<string>>: dictionary
	'''
	#Current dir will be where the spiders are
	df= pd.read_csv('resource_uri/'+ table, sep=';', index_col=None, header=0, encoding= 'utf-8')		
	result={
		key:value
			for key, value in
				zip(df[key_column],df[resource_uri_column])}

	return result 			
