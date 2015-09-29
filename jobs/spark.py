__author__ = 'lucas'


es_read_conf = {        "es.nodes" : "elasticsearch",        "es.port" : "9200",        "es.resource" : "secse/monologue"    }
es_write_conf = {        "es.nodes" : "elasticsearch",        "es.port" : "9200",        "es.resource" : "secse/monologue"    }
es_rdd = sc.newAPIHadoopRDD(        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",        keyClass="org.apache.hadoop.io.NullWritable",        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",        conf=es_read_conf)


#queries

es_rd_rdd.map(lambda a: (a[1]['owner_user_name'],1)).reduceByKey(lambda x,y:x+y).takeOrdered(25,key = lambda x: -x[1]).first()
