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


## Assumption Max capacity is always 100 in capacity scheduler

## percentage in fair is of cluster resources

## Imports
import sys
import re
from xml.dom import minidom
import xml.etree.ElementTree as ET


def read_fair_xml(fair_scheduler, capacity_scheduler):

	tree = ET.parse(fair_scheduler)
	root = tree.getroot()
	fair_list = []
	for queue in root.iter("queue"):
		maxResources = queue.find("maxResources")
		try:
			fair_list.append((queue.get("name"), maxResources.text))
		except:
			pass
	write_to_capacity_scheduler(fair_list, capacity_scheduler)


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
							print capacity.text
							match = re.search("root\.(.+?)\.capacity", grand_child.text)
							if match: ## Root pool gets ignored
								pool  = match.group(1)
								sub_pool = pool.split(".")
								cap_list.append((sub_pool,capacity.text))
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
	link_lists(fair_list, cap_list)
	

def link_lists (fair_list, cap_list):

	print fair_list
	print cap_list

	for pool in fair_list:
		name=pool[0]
		values=pool[1]
		print name, values 



	#print root.tag, root.attrib
	#for queue in root.iter("name"):
		#print queue.text
		#val = root.iter("value")
		#print val.text



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