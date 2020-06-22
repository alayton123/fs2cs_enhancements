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


## TODO - Add support for non distinct naming (ie there is no pool user.alice users2.alice)
## TODO - Is support needed for the root pool?
## TODO - Text for next layer down in fair (layer.layer1.layer2) etc

## Assumption Max capacity is always 100 in capacity scheduler

## percentage in fair is of cluster resources

## Root top level pool max resources get ignored

## Imports
import sys
import re
from xml.dom import minidom
import xml.etree.ElementTree as ET





def write_to_capacity_scheduler(fair_list, capacity_scheduler):
	
	tree = ET.parse(capacity_scheduler)
	root = tree.getroot()
	cap_list =[]
	for child in root:
			for grand_child in child:
				#Filter for the capacity relavent entries
				if "yarn.scheduler" in grand_child.text:
					lis = grand_child.text.split(".")
					if "capacity" in lis[-1]:
						print grand_child.text
						for capacity in child.iter("value"):
							match = re.search("root\.(.+?)\.capacity", grand_child.text)
							if match: ## Root pool gets ignored
								pool  = match.group(1)
								sub_pool = pool.split(".")
								cap_list.append((sub_pool,capacity.text))
	
	link_lists(fair_list, cap_list)
	

def link_lists (fair_list, cap_list):

	#print fair_list
	#print cap_list

	for pool in fair_list:
		print pool[1]
		#for pool2 in cap_list:
		#	cap_name=pool2[0]
		#	cap_value=pool2[1]
			#print str(fair_name) + " ----------- " + str(cap_name) 



	#print root.tag, root.attrib
	#for queue in root.iter("name"):
		#print queue.text
		#val = root.iter("value")
		#print val.text



def read_fair_xml(fair_scheduler, capacity_scheduler):

	root = ET.parse(fair_scheduler)

	## Top layer is root. Root needs to be iterated over to begin.
	root_layer = root.findall("./queue")
	for i in root_layer:
		main_layers = i.findall("./queue")

	fair_list = []

	for i in main_layers:
		print str(i.get("name"))+ " top level"
		queue_list = make_recursive(i)
		for i in queue_list:
			fair_list.append(i)

	write_to_capacity_scheduler(fair_list, capacity_scheduler)		

def make_recursive(layer):

	return_list=[]

	## Ignore parents with no max resources
	maxResources = layer.find("maxResources")
	if maxResources is not None: 
		return_list.append ([layer.get("name"), maxResources.text])

	## Gets parent tag for next stage
	parent=str(layer.get("name"))

	for i in layer.findall(".queue"):
		child = str(i.get("name"))
		childResources = i.find("maxResources")
		if childResources is not None:
			return_list.append(([parent + "." + child, childResources.text]))

	return return_list
		


read_fair_xml(sys.argv[1], sys.argv[2])

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




		#print queue.tag , queue.attrib
		



	#for child in root:
	#	print "11111", child.tag, child.attrib
	#	try_next_layer(child)


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
	write_to_capacity_scheduler(fair_list, capacity_scheduler)