### spusteni Sparku
# pyspark --num-executors 2 --executor-memory 1500M --packages com.databricks:spark-csv_2.10:1.5.0 --master yarn

# potlaceni vypisu INFO logu
sc.setLogLevel("WARN")

#### vypocet statu s nejvyssi prumernou teplotou pomoci Spark RDD
def uprav_radek(r):
    r2 = r.split(',')
    stat = r2[9]
    tepl = (int(r2[4])/10.0 - 32)*5/9
    return (stat, (tepl, 1))

def soucty(a, b):
    soucetA = a[0]
    soucetB = b[0]
    pocetA = a[1]
    pocetB = b[1]
    return (soucetA + soucetB, pocetA + pocetB)

# cteni teplot ze souboru
teploty_raw = sc.textFile('/user/pascepet/teplota')
# vyhodi se hlavicky a neplatne + nepotrebne udaje
teploty_raw = teploty_raw.filter(lambda r:
    (r.split(',')[1] in set('678')) & (r.split(',')[4] != ''))
# ponechaji se jen potrebna data
teploty = teploty_raw.map(uprav_radek)
# agregace po statech, vypocet prumeru, serazeni
teploty_staty = teploty.reduceByKey(soucty) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .sortBy(lambda y: y[1], False)

# vypise poradi statu
teploty_staty.take(1)
teploty_staty.collect()
####################################
    
#### vypocet statu s nejvyssi teplotou pomoci Spark SQL
from pyspark.sql import Row

def uprav_radek_df_row(r):
    r2 = r.split(',')
    stat = r2[9]
    tepl = (int(r2[4])/10.0 - 32) * 5/9
    return Row(stat=stat, tepl=tepl)

# cteni teplot ze souboru
teploty_raw = sc.textFile('/user/pascepet/teplota')
# vyhodi se hlavicky a neplatne + nepotrebne udaje
teploty_raw = teploty_raw.filter(lambda r:
     (r.split(',')[1] in set('678')) & (r.split(',')[4] != ''))

### transformace do DataFrame, kde sqlContext si sam odvodi datove typy
teploty_prep = teploty_raw.map(uprav_radek_df_row)
teplotyDF = sqlContext.createDataFrame(teploty_prep)

### agregace po statech, vypocet prumeru, serazeni
teploty_statyDF = teplotyDF.groupBy('stat').avg('tepl') \
    .toDF('stat', 'tepl_prum')

teploty_statyDF = teploty_statyDF.sort(teploty_statyDF.tepl_prum.desc())

# vypise poradi statu
teploty_statyDF.show(100)
#############################

### totez pomoci Spark SQL a s definovanym schematem a docasnou registrovanou tabulkou
from pyspark.sql.types import *

def uprav_radek_df(r):
    r2 = r.split(',')
    stat = r2[9]
    tepl = (int(r2[4])/10.0 - 32) * 5/9
    return (stat, tepl)

# cteni teplot ze souboru
teploty_raw = sc.textFile('/user/pascepet/teplota')
# vyhodi se hlavicky a neplatne + nepotrebne udaje
teploty_raw = teploty_raw.filter(lambda r:
     (r.split(',')[1] in set('678')) & (r.split(',')[4] != ''))

### transformace do DataFrame s definovanym schematem
teploty_prep2 = teploty_raw.map(uprav_radek_df)
# definice schematu
teploty_pole = [StructField('stat', StringType(), True),
    StructField('tepl', DoubleType(), True)]
teploty_schema = StructType(teploty_pole)
# pouziti schematu
teplotyDF2 = sqlContext.createDataFrame(teploty_prep2, teploty_schema)

# registrace DataFrame jako tabulky
teplotyDF2.registerTempTable("teploty")

### agregace pomoci registrovane tabulky
teploty_statyDF2 = sqlContext.sql("""select stat, avg(tepl) as tepl_prum from teploty
group by stat order by tepl_prum desc""")
teploty_statyDF2.show(100)
##############################

### vypocet pomoci Spark SQL, kde se dataframe rovnou nacte z CSV
teploty_DF3 = sqlContext.read \
	.format("com.databricks.spark.csv") \
	.option("header", "true") \
	.option("delimiter", ",") \
	.option("inferSchema", "true") \
	.load("/user/pascepet/teplota")

teploty_DF3 = teploty_DF3.filter((teploty_DF3.mesic>5) & (teploty_DF3.mesic<9)) \
    .select('stat','teplota').na.drop()
teploty_DF3 = teploty_DF3.withColumn('teplota', (teploty_DF3.teplota/10.0 - 32) * 5/9)
teploty_DF3 = teploty_DF3.groupBy('stat').avg() \
    .toDF('stat','prum')
teploty_DF3.sort(teploty_DF3.prum.desc()).limit(1).show()

#### alternativne nacteni dataframe z Hive
teploty_DF4 = sqlContext.sql('select * from temperature')
teploty_DF4 = teploty_DF4.filter((teploty_DF4.mesic>5) & (teploty_DF4.mesic<9)) \
    .select('stat','teplota').na.drop()
# teploty_DF4 = teploty_DF4.withColumn('teplota', (teploty_DF4.teplota/10.0 - 32) * 5/9)
# neni treba, v tabulce je jiz prevedeno na stupne Celsia
teploty_DF4 = teploty_DF4.groupBy('stat').avg() \
    .toDF('stat','prum')
teploty_DF4.sort(teploty_DF4.prum.desc()).limit(1).show()
#############################