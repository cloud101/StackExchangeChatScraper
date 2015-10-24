from elasticsearch import Elasticsearch
from collections import Counter
from bs4 import BeautifulSoup
from pyspark import SparkContext, SparkConf

appName = "secse" 
master = "spark://ubuntu:7077"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

es = Elasticsearch(['http://elasticsearch:9200'])
es_read_conf = {        "es.nodes" : "elasticsearch",        "es.port" : "9200",        "es.resource" : "secse/monologue"    } 
es_rdd = sc.newAPIHadoopRDD(        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",        keyClass="org.apache.hadoop.io.NullWritable",        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",        conf=es_read_conf) 

def add_imgs_es(img_list):
	for img in img_list:
		if img.startswith('//'):
			img.replace('//','http://')
		mapped_img = {'img_url':img}
		index_message('results','image_url',mapped_img,mapped_img['img_url'])

	
def extract_image_files(content):
	res = list()
	if 'img' in content.lower():
		soup = BeautifulSoup(content)
		images = soup.findAll('img')
		for img in images: 
			res.append(img.get('src'))
	return res
	

def message_counter(content,message):
	return message.lower().count('donut')
	

def index_message(index,type,message,id=None):
	if id:
		es.index(index=index, doc_type=type, id=id, body=message)
	else:	
		es.index(index=index, doc_type=type,  body=message)

		
def add_top_to_es(top_list,type):
	x = 1
	for tup in top_list:
		res_dict = dict()
		res_dict["author"] = tup[0] 
		res_dict["count"] = tup[1]
		x = x + 1
		index_message('results',type,res_dict,x)
	




add_top_to_es( es_rdd.map(lambda a: (a[1]['owner_user_name'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1]),'top25_posters')
message_rdd = es_rdd.map(lambda a: (a[1]['owner_user_id'],a[1]['owner_user_name'],a[1]['content'],a[1]['id'])).cache()
count_donuts = message_rdd.map(lambda a: message_counter(a[2],'donut'))
count_donuts_per_author = message_rdd.map(lambda a: (a[1],message_counter(a[2],'donut'))).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
images = message_rdd.map(lambda a: extract_image_files(a[2])).filter(lambda a: len(a) > 0 )
images_list = images.collect()
for img_list in images_list:
	add_imgs_es(img_list)
count_your_mom_per_author = message_rdd.map(lambda a: a[1],message_counter(a[2],'your mom')).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
add_top_to_es( count_your_mom_per_author,'top25_yourmom')
count_simons_mom_per_author = message_rdd.map(lambda a: a[1],message_counter(a[2],"simon's mom")).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
add_top_to_es( count_simons_mom_per_author,'top25_simonsmom')
count_pls = message_rdd.map(lambda a: a[1],message_counter(a[2],"pls")).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
add_top_to_es( count_pls,'top25_count')
count_fuck = message_rdd.map(lambda a: a[1],message_counter(a[2],"fuck")).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
add_top_to_es( count_fuck,'top25_fuck')
count_shit = message_rdd.map(lambda a: a[1],message_counter(a[2],"shit")).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1])
add_top_to_es( count_shit,'top25_shit')