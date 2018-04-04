# -*- coding: utf-8 -*-
'''
	Date: Dec 20th, 2017

	Author: Guilherme Varela

	ref: https://doc.scrapy.org/en/1.4/intro/tutorial.html#intro-tutorial

	Transforms json into a csv table
'''

import json 

def json2csv(path2json, outfilename):
	'''
		Reads the json file, and outputs a csv file 
		json file is an array of dicts

		INPUT
			path2json<string> This is the path to a jsonfile 
			
			outfilename<string> This is the output path to a json			

		OUTPUT

	'''
	this_dict=json.loads(path2json)

if __name__=='__main__':
	json2csv('/datasets/congressman_and_party_membership.json','/resource_uri/congressman_party_membership.csv')	



