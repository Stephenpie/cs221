import json
import os

#{"invalid_count": 0, "subdomain": {}, "MaxOutputLink": ["", -1], "pages_count": 0}
def save_data(file_name, pages_count, MaxOutputLink, invalid_count, subdomain):
	#with open (file_name, 'w') as json_file:
	data = {}
	data['pages_count']= pages_count
	data['MaxOutputLink'] = MaxOutputLink
	data['invalid_count'] = invalid_count
	data['subdomain'] = subdomain
	#data['valid_link'] = valid_link
	with open(file_name,'w') as outfile:
	    json.dump(data, outfile)


# file_name = data.txt
def load_data(file_name):
	if os.path.exists(file_name):
		with open (file_name) as json_file:  
		    data = json.load(json_file)
		    MaxOutputLink = []
		    subdomain = dict()

		    pages_count = data['pages_count']
		    for p in data['MaxOutputLink']:
		        MaxOutputLink.append(p)
		    invalid_count = data['invalid_count']
		    subdomain = data['subdomain']#
	return pages_count, MaxOutputLink, invalid_count, subdomain