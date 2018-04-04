# -*- coding: utf-8 -*-
'''
    Date: Mar 31th, 2018

    Author: Guilherme Varela

    Silly script that generates 315 posts
'''
from uuid import uuid4

with open('datasets/posts-55.csv', 'w') as f:
    f.write('slp:resource_uri\n')
    for _ in range(315):
        f.write('{:}\n'.format(str(uuid4())))
