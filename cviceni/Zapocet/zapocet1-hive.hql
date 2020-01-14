-- Zapoctovy test leden 2020
-- reseni pro Hive

-- spusteni Hive na clusteru
-- beeline -u \"jdbc:hive2://hador-c1.ics.muni.cz:10000/default;principal=hive/hador-c1.ics.muni.cz@ICS.MUNI.CZ\"

-- vytvorit externi tabulku
create external table games_ext (
white_name string,
black_name string,
white_rating int,
black_rating int,
date_played string,
result string,
tags string,
moves string,
white_pts double,
game_len int
)
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '~'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/user/pascepet/data/kingbase'
tblproperties ("skip.header.line.count"="1")
;

-- nepovinna, ale uzitecna kontrola
select count(*) from games_ext; -- 1 861 426 zaznamu
select * from games_ext limit 5; -- melo by ukazat smysluplny obsah

-- vytvorit managed tabulku
create table games (
white_name string,
black_name string,
white_rating int,
black_rating int,
date_played string,
result string,
tags string,
moves string,
white_pts double,
black_pts double,
game_len int
)
stored as parquet
tblproperties ("parquet.compress"="GZIP")
;

-- naplnit managed tabulku z externi 
insert overwrite table games
select 
white_name,
black_name,
white_rating,
black_rating,
date_played,
result,
tags,
moves,
white_pts,
1-white_pts black_pts,
game_len
from games_ext
where white_rating>=2500 and black_rating>=2500 and game_len>=30;

-- opet nepovinna, ale uzitecna kontrola
select count(*) from games; -- 191 769 zaznamu
select * from games limit 5;

-- zahodi se externi tabulka
drop table games_ext;

-- dotazy do Hive...
-- cetnost vysledku
select result, count(*) pocet
from games
group by result
;

+----------+--------+--+
|  result  | pocet  |
+----------+--------+--+
| 0-1      | 37977  |
| 1-0      | 60242  |
| 1/2-1/2  | 93550  |
+----------+--------+--+


-- pet hracu s nejvyssim poctem odehranych her cernymi
select black_name, count(*) pocet
from games
group by black_name
order by pocet desc
limit 5
;

+---------------------+--------+--+
|     black_name      | pocet  |
+---------------------+--------+--+
| Ivanchuk, Vassily   | 1608   |
| Shirov, Alexei      | 1366   |
| Anand, Viswanathan  | 1333   |
| Gelfand, Boris      | 1290   |
| Svidler, Peter      | 1269   |
+---------------------+--------+--+
