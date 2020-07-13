## alayton 02/06/20
## This program is designed to supplement the internal fs2cs code and convert the ax resource absolute values in FairScheduler resource pools to % of total cluster resources for the capcity scheduler.

####### LOGIC #########

## !!!Back up fair scheduler XML!!!!
## Take Fair Scheduler xml and get resource pool stats
## Use API to get view of cluster (YARN MEM/CORES)
## Parse and calculate % for the resource pools
## Test to make sure percentage is not less than min.
## Rebuild % resource pools in capacity scheduler XML
## Output XML or list of resource pools and absolute to capacity %s 


## Required run "pip install requests"
## Required run "pip install cm_client"

## TODO - Add support for non distinct naming (ie there is no pool user.alice users2.alice)
## TODO - Is support needed for the root pool?
## TODO - Test for next layer down in fair (layer.layer1.layer2) etc
## TODO- Add logging? Worth it...?

## Assumption Max capacity is always 100 in capacity scheduler
## Assumption max capacity in cap scheduler sub pool can be larger than max capacity in parent pool
## Assumption Fair scheduler max capacity is in % or absolute only for both cores and mem. Not both.
## Assumption Fair scheduler absoluite is always in "mb" and vcores and always in the same order (eg users2.dave    20480 mb, 8 vcores)

## Capacity scheduler max capacity is not split into cores and memory as fair scheduler was. So the tool takes the largest value of two when doing the conversion..

## percentage in fair is of cluster resources

## Root top level pool max resources get ignored in capacity and fair scheduler

## Imports
import sys
import re
#import requests
import cm_client
from xml.dom import minidom
import xml.etree.ElementTree as ET


def read_capacity_scheduler(capacity_scheduler):
	
	tree = ET.parse(capacity_scheduler)
	root = tree.getroot()
	cap_list =[]
	for child in root:
			for grand_child in child:
				#Filter for the capacity relavent entries
				if "yarn.scheduler" in grand_child.text:
					lis = grand_child.text.split(".")
					if "capacity" in lis[-1]:
						for capacity in child.iter("value"):
							match = re.search("root\.(.+?)\.capacity", grand_child.text)
							if match: ## Root pool gets ignored
								pool  = match.group(1)
								#sub_pool = pool.split(".")
								cap_list.append((pool,capacity.text))
	
	return cap_list
	

def link_lists (fair_list, cap_list):

	## Lists may be different lengths as cap list has max resources for all but fair scheduler has max resources only for some.
	perc_out_list = [] ## List for things with the "%" absolutes will be handled differently.

	for pool in fair_list:
		fair_name=pool[0]
		fair_value=pool[1]

		## filter for % vs absolutes 
		## Max resources is 100% by default in capacity scheduler. Therefore we don't care about the existing capacity and can do a straight conversion 
		if "%" in fair_value:
			
			perc_output = percentage_values(fair_name, fair_value, cap_list)
			perc_out_list.append(perc_output) ## Produce list to feed back to generate new xml

		else: ## Absolute values
			absolute_values(fair_name, fair_value, cap_list)

	#print perc_out_list
				
			
def absolute_values(fair_name, fair_value, cap_list):

	print fair_name + "    " + fair_value

	## clean to get the numeric values for mb and cores
	split = fair_value.split(",")
	mb = re.findall(r"\d+", split[0]) ## pulls all numeric values from fair scheduler max resources
	cores= re.findall(r"\d+", split[1])
	
	print mb
	print cores

def percentage_values(fair_name, fair_value, cap_list):

	numeric = re.findall(r"\d+", fair_value) ## pulls all numeric values from fair scheduler max resources
	if len(numeric) == 2:
		max_value = numeric[0]
		#print fair_name + ":" +  numeric[0]
	elif len(numeric) == 4: ## Is possible for there to be a max value for cpu and max value for core. In this case we take larger of the two as the capacity scheduler has a single max resources value.
		## Take the larger value of max cpu or max memory
		if numeric[0] > numeric[2]:
			max_value = numeric[0]
		else:
			max_value = numeric[2]
		#print fair_name + ":" + max_value
#print fair_name + ":" + max_value	

	for pool2 in cap_list:
		cap_name=pool2[0]
		if fair_name == cap_name: ## Test for matching pools (don't need to worry about cap_value as always 100% by default )
			return (cap_name, max_value)

def read_fair_xml(fair_scheduler):

	root = ET.parse(fair_scheduler)
	## Top layer is root. Root needs to be iterated over to begin.
	
	child_list=[]
	fair_list = []

	#print "ROOT 1 ----------------"
	root_layer = root.findall("./queue") # This returns the root queue (parent of all other queues)

	#print "LAYER 2 ----------------"
	for elements in root_layer:
		child_ele= elements.findall("./queue") # Returns all layer 2 queues
		for child in child_ele:
			queue_list = make_recursive(child) # Passes the layer 2 queues into the reciursive function which checks further for sub-queues and returns any queue or sub quue wqith max_resources populated.
			for queue_and_max in queue_list:
				fair_list.append(queue_and_max)

	return fair_list		

def make_recursive(layer):

	return_list=[]

	## Ignore parents with no max resources
	maxResources = layer.find("maxResources")
	if maxResources is not None: 
		return_list.append ((layer.get("name"), maxResources.text))

	## Gets parent tag for next stage
	parent=str(layer.get("name"))

	for i in layer.findall(".queue"):
		child = str(i.get("name"))
		childResources = i.find("maxResources")
		if childResources is not None:
			return_list.append((parent + "." + child, childResources.text))

	return return_list


def cluster_stats():
	"""Gets the total YARN meory & VCores using the Cloudera anager API"""

	#req = requests.get("http://nightly6x-unsecure-1.nightly6x-unsecure.root.hwx.site:7180/api/v40/tools/echo?message=hello")
	#print req.content

	cm_client.configuration.username = 'admin'
	cm_client.configuration.password = 'admin'

	# Create an instance of the API class
	api_host = 'http://nightly6x-unsecure-1.nightly6x-unsecure.root.hwx.site'
	port = '7180'
	api_version = 'v33'
	# Construct base URL for API
	# http://cmhost:7180/api/v30
	api_url = api_host + ':' + port + '/api/' + api_version
	print api_url
	api_client = cm_client.ApiClient(api_url)
	cluster_api_instance = cm_client.ClustersResourceApi(api_client)

	api_response = cluster_api_instance.read_clusters(view='SUMMARY')
	for cluster in api_response.items:
		print cluster.name, "-", cluster.full_version

	services_api_instance = cm_client.ServicesResourceApi(api_client)
	services = services_api_instance.read_services(cluster.name, view='FULL')

	for service in services.items:
		
		if service.type=="YARN":
			yarn = service

	print yarn.name
    

	api_url_v5 = api_host + '/api/' + 'v5'
	api_client_v5 = cm_client.ApiClient(api_url_v5)
	print api_url_v5
	services_api_instance_v5 = cm_client.ServicesResourceApi(api_client_v5)
	#print services_api_instance_v5.get_metrics(cluster.name, hdfs.name)
	metrics = services_api_instance_v5.get_metrics(cluster.name, yarn.name)
	for m in metrics.items:
		print "%s (%s)" % (m.name, m.unit)




def main(fair_scheduler, capacity_scheduler):
	## Main orchestration function

	fair_list = read_fair_xml(fair_scheduler)
	cap_list = read_capacity_scheduler(capacity_scheduler)

	cluster_stats()

	#link_lists(fair_list,cap_list)




main(sys.argv[1], sys.argv[2])

def read_data():

	## prompt for user input to get absolute values from Fair Scheduler resource pools 

	min_cores = input("Min cores:")
	max_cores = input("Max cores:")
	
	min_mem = input("Min memory:")
	max_mem = input("Max memory:")

	total_cores = input("Cluster total cores:")
	total_mem = input("Cluster total memory:")

	do_perc_calcs(min_cores, max_cores, min_mem, max_mem, total_cores, total_mem)

def do_perc_calcs(min_cores, max_cores, min_mem, max_mem, total_cores, total_mem):

	min_perc = min_cores/total_cores 
	print (min_perc)

def try_next_layer(child):
	## pick up all nested queues
	for grand_child in child:
		print "22222", grand_child.tag, grand_child.attrib

def read_xml2(file):

	xmldoc=minidom.parse(file)
	queue_list = xmldoc.getElementsByTagName('queue')
	for type_tag in queue_list:
		if type_tag.attributes['maxResources'].value:
			print(type_tag.attributes['name'].value)
                	print(type_tag.attributes['weight'].value)
		else:
			print ("no")
			print(type_tag.attributes['name'].value)


		#print type_tag
		#val = type_tag.get("alice")
		#print(type_tag.attributes['name'].value)
		#print(type_tag.attributes['weight'].value)




			#if len(layer) == 0:
		#pass

	#else:
		#check_layers(layer1)

	#for z in layer1:
	#	layer2 = z.findall("./queue")
	#	for c in layer2:
	#		print c.get("name")
	#		layer3 = c.findall("./queue")
	#		for v in layer3:
	#			print v.get("name")
	#			layer4 = v.findall("./queue")
	#			if len(layer4) == 0:
	#				print "End"
				

	#x = root.findall(".//queue/..[maxResources]") ## All queues which have max resources....
	#for i in x:
		#print i.tag, i.attrib
		#maxResources = i.find("maxResources")
		#print maxResources.text

#def check_layers(layer):
	## Recursively checks layers to build view of element tag

#	for i in layer:
#		print str(i.get("name")) + " HEREEE"
#		next_layer = i.findall("./queue")
#		return(next_layer)


#print cap_list


				#print (child.text.split("."))[-1:]
				#if (grand_child.text.split("."))[-1:] == "capacity":
				#	print grand_child.text
				#	for val in child.iter("value"):
				#		print val.text
				#if "maximum-capacity" in grand_child.text:
				#		for max_perc in child.iter("value"):
				#			match = re.search("root\.(.+?)\.maximum-capacity", grand_child.text)
				#			if match: ## Root pool gets ignored
				#				pool  = match.group(1)
				#				sub_pool = pool.split(".")
				#				cap_list.append(sub_pool)



				#def read_fair_xml(fair_scheduler, capacity_scheduler):

	#tree = ET.parse(fair_scheduler)
	#root = tree.getroot()
	#fair_list = []
	#for queue in root.iter("queue"):
	#	maxResources = queue.find("maxResources")
	#	try:
	#		print queue.get("name")
	#		fair_list.append((queue.get("name"), maxResources.text))
	#	except:
	#		pass
	#write_to_capacity_scheduler(fair_list, capacity_scheduler)

	#print pool[0], pool[1]
				#print pool2[0], pool2[1]

		#for pool2 in cap_list:
		#	cap_name=pool2[0]
		#	cap_value=pool2[1]
			#print str(fair_name) + " ----------- " + str(cap_name) 



	#print root.tag, root.attrib
	#for queue in root.iter("name"):
		#print queue.text
		#val = root.iter("value")
		#print val.text




		#print queue.tag , queue.attrib
		



	#for child in root:
	#	print "11111", child.tag, child.attrib
	#	try_next_layer(child)