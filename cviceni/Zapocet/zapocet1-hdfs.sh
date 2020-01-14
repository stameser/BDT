# Zapoctovy test leden 2020
# HDFS operace

# prava read a execute na uzivatelskem adresari
hdfs dfs -chmod o+rx /user/username
# nebo (pri pocatecnim stavu 750)
hdfs dfs -chmod 755 /user/username

# vytvoreni adresare kingbase
hdfs dfs -mkdir /user/username/kingbase
# nebo (na HDFS jsou relativni cesty vzdy z vlastniho user adresare)
hdfs dfs -mkdir kingbase

# zkopirovani dat
hdfs dfs -put /home/pascepet/fel_bigdata/data/kingbase/KingBase2016-03-E* kingbase
