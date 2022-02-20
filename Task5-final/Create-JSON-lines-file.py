'''
Petio Todorov
Wael Fato
25/11/20

Create-JSON-lines-file.py

The code below was used to convert the json file (arxivData.json) into json lines (.jl) format
so it can be read into the mapper properly. Thanks to Xiangyu for the recommendation! It needs to be run only once.

The following source was used as a basis for the code:
https://blog.softhints.com/python-convert-json-to-json-lines/#convertnormaljsontojsonlonlywithpython

To RUN: Just run it like a regular file in Pycharm

Input-
1. A json file, arxivData.json

Output-
1. A json lines file, mod-arxiv.jl
'''

import json

with open('arxivData.json','r') as file:
    # load requires a file as an input, loads requires a string input (e.g file.read())
    data = json.load(file) # this is a list of dictionaries

# create a new file and add the list of dictionaries
with open('mod-arxivData.txt', 'w') as new_file:
    for line in data:
        json.dump(line, new_file)
        new_file.write('\n')