### Ulohy pro zapoctovy test, leden 2020
# pomoci Sparku
# pyspark --master yarn --num-executors 4 --executor-memory 4G --packages com.databricks:spark-csv_2.10:1.5.0 

import re
from pyspark.sql import functions as F

### ulohy Spark RDD
# nacteni dat
kbRDD = sc.textFile("/user/pascepet/data/kingbase")

# pet nejcastejsich kombinaci prvnich dvou tahu
kbRDD2 = kbRDD.map(lambda g: g.split("~")[7]).filter(lambda m: m!="game") # z vychoziho RDD se ponecha jen zaznam tahu hry
kbRDD2.cache() # nepovinne, ale uzitecne

kbRDD3 = kbRDD2.map(lambda m: ' '.join(m.split(" ")[:2])) # nebo m.split(" ")[0]+m.split(" ")[1]
kbRDD3.map(lambda mm: (mm, 1)).reduceByKey(lambda a,b:a+b).sortBy(lambda kv: kv[1], False).take(5)
# -> [(u'1.e4 c5', 401324), (u'1.d4 Nf6', 376137), (u'1.d4 d5', 176085), (u'1.e4 e5', 167575), (u'1.e4 e6', 113821)]

# kolik her skoncilo matem (znak '#' v zapisu tahu, vzdy na konci posledniho tahu pred vysledkem)
kbRDD2.filter(lambda m: '#' in m).count()
# nebo sloziteji kbRDD2.filter(lambda m: re.search('#', m)!=None).count()
# -> 24 230

# kolik tahu jezdcem (tah obsahuje pismeno 'N') celkem ve hrach, kde oba hraci meli rating nad 2600
kbRDD2 = kbRDD.map(lambda g: g.split("~")) \
    .filter(lambda g: g[0] != "white") \
    .filter(lambda g: int(g[2])>2600) \
    .filter(lambda g: int(g[3])>2600)
kbRDD3 = kbRDD2.map(lambda g: g[7]).flatMap(lambda m: m.split(" "))
kbRDD3.filter(lambda m: re.search('N', m)!=None).count()
# -> 857 889

### ulohy Spark SQL
# nacteni dat
kbDF = sqlContext.read \
	.format("com.databricks.spark.csv") \
	.options(header="true", delimiter = "~", inferSchema = "true") \
	.load("/user/pascepet/data/kingbase")

kbDF.cache() # nepovinne, ale uzitecne
# kolik partii odehral hrac Nakamura, Hikaru (dohromady jako bily i jako cerny) v roce 2015?
kbDF.filter(F.substring(kbDF.date, 1, 4)=='2015').filter('white="Nakamura, Hikaru" or black="Nakamura, Hikaru"').count()
# -> 162

# jaky je prumerny pocet bodu pro bileho v partiich, kde se ratingy obou hracu lisi nejvyse o 50 bodu?
kbDF.filter(F.abs(kbDF.white_rating-kbDF.black_rating)<=50.0).groupBy().avg('points').show()
# -> 0.544

# hra s nejvetsim poctem tahu, souperi, vysledek
kbDF.orderBy(kbDF.game_len.desc()).select('white', 'black', 'result', 'game_len').show(1)
# -> Felber, Joseph J|Lapshun, Yury|1/2-1/2|     475|

