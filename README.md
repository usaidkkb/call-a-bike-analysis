## An Apache Spark learning project to analyse Call a Bike booking data.

This project is created as a practical example of learning how to use Apache Spark to perform some basic analysis on a dataset.

The dataset is a CSV file which contains almost 12 million bookings in time period (2014-2016) of one of 
Germany's biggest bike sharing platform.

The spark job runs a couple of different Transformations which are listed below:
1) Top cities with the most bookings
2) Top stations in Hamburg where the trip started
3) Top stations in Hamburg where the trip ended
4) Top routes in Hamburg (Most co-occurring start and end stations)
5) Average trip length per day

### How to run
###### Requirements: 
1) Java 8
2) Scala 2.11.8
3) Apache Spark v2.2.0 https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
4) https://data.deutschebahn.com/dataset/data-call-a-bike/resource/b51f1366-15a1-4176-bbc0-74c2722faf9c

###### Run using `spark-submit`:
1) Set or replace SPARK_HOME with the directory which contains your downloaded and unpacked Spark binaries.
2) Set or replace DATASET_PATH with the directory which contains your downloaded dataset (Requirement no.4)
```
git clone https://github.com/usaidkkb/call-a-bike-analysis.git
cd call-a-bike-analysis
HOME=${pwd}
sbt assembly
$SPARK_HOME/bin/spark-submit --class "CallABikeAnalysis" \
  $HOME/target/scala-2.11/call-a-bike-analysis-assembly-0.1.jar \
  --input-file $DATASET_PATH/HACKATHON_BOOKING_CALL_A_BIKE.csv \
  --output-dir $HOME/call-a-bike-analysis/output
```

### Output
Output of the `CallABikeAnalysis` job can be found in `output`. All transformation results are in their respective 
directories. The file that contains the results is in the format `part-*.txt`
1) Top cities with the most bookings
```
cd output/topCities

city,count
Hamburg,6505521
Frankfurt am Main,1340675
Berlin,1008282
M�nchen,800808
Kassel,551420
Stuttgart,461370
K�ln,447321
Darmstadt,272471
Marburg,195326
R�sselsheim,65339
Wiesbaden,29079
S+U-Bhf. Alexanderplatz - Alexa,9730
Flugh. Frankfurt,7582
Halle,2117
Kiel,1724
Baden-Baden,1549
Mannheim,1530
U-Bhf.Alexanderplatz /,1503
Hannover,1453
Heidelberg,1097
```
2) Top stations in Hamburg where the trip started
```
cd output/top20HamburgStartStations

startStation,count
U Bahn Lattenkamp,7697
Museumshafen,7279
U-Bahn,5666
MiniaturWunderland,5352
Grindelberg / Bezirksamt Eimsb�ttel,4319
Bahnhof Wilhelmsburg,2215
S-Bahrenfeld,1506
U Bahn �berseequartier,1263
S-Othmarschen,1256
S Bahn Ohlsdorf,1141
Mobile Station D2 Karte,791
F R 5156,634
Test Terminal BSC HH Wandalenweg,411
BSCTerminal FFM 2.OG R 2.21,73
BSC Hamburg / Hammerbrook,52
Hafentor / Landungsbr�cken,20
BSC Testterminal Halle,13
FAHNDUNG,12
Pop Bike Standort Mainzer Landstra�e 169 / Raum 802,11
Rotec Schl�sser BCS,9
```
3) Top stations in Hamburg where the trip ended
```
cd output/top20HamburgEndStations

endStation,count
U Bahn Lattenkamp,7422
Museumshafen,7368
U-Bahn,5434
MiniaturWunderland,5295
Grindelberg / Bezirksamt Eimsb�ttel,4292
Bahnhof Wilhelmsburg,2203
S-Bahrenfeld,1457
U Bahn �berseequartier,1250
S-Othmarschen,1174
S Bahn Ohlsdorf,1126
Test Terminal BSC HH Wandalenweg,1086
Mobile Station D2 Karte,832
F R 5156,636
BSCTerminal FFM 2.OG R 2.21,90
BSC Hamburg / Hammerbrook,22
Hafentor / Landungsbr�cken,20
BSC Testterminal Halle,12
Pop Bike Standort Mainzer Landstra�e 169 / Raum 802,11
FAHNDUNG,11
unbekannt,9
```
4) Top routes in Hamburg (Most co-occurring start and end stations)
```
cd output/topRoutesHamburg

start,end,count
U Bahn Lattenkamp,U Bahn Lattenkamp,7034
Museumshafen,Museumshafen,6603
U-Bahn,U-Bahn,4770
MiniaturWunderland,MiniaturWunderland,4730
Grindelberg / Bezirksamt Eimsb�ttel,Grindelberg / Bezirksamt Eimsb�ttel,3776
Bahnhof Wilhelmsburg,Bahnhof Wilhelmsburg,2156
S-Bahrenfeld,S-Bahrenfeld,1079
U Bahn �berseequartier,U Bahn �berseequartier,1062
S Bahn Ohlsdorf,S Bahn Ohlsdorf,1000
S-Othmarschen,S-Othmarschen,906
Mobile Station D2 Karte,Mobile Station D2 Karte,658
F R 5156,F R 5156,518
Test Terminal BSC HH Wandalenweg,Test Terminal BSC HH Wandalenweg,404
U-Bahn,Grindelberg / Bezirksamt Eimsb�ttel,218
U-Bahn,MiniaturWunderland,194
U-Bahn,Museumshafen,181
Grindelberg / Bezirksamt Eimsb�ttel,U-Bahn,177
S-Bahrenfeld,S-Othmarschen,169
MiniaturWunderland,Museumshafen,156
S-Othmarschen,Museumshafen,156
```
5) Average trip length per day

note: dataset is ordered by average trip length in ascending order to find days 
with the shortest trips
```
cd output/averageTripLengthByDay

[2015-02-04 01:00:00.0,2015-02-05 01:00:00.0] 12.624328924970538
[2015-02-03 01:00:00.0,2015-02-04 01:00:00.0] 12.70247263883259
[2014-12-02 01:00:00.0,2014-12-03 01:00:00.0] 13.629300776914539
[2016-02-09 01:00:00.0,2016-02-10 01:00:00.0] 13.77499219806512
[2014-12-01 01:00:00.0,2014-12-02 01:00:00.0] 14.575663026521061
[2015-11-30 01:00:00.0,2015-12-01 01:00:00.0] 14.682303911092113
[2014-12-17 01:00:00.0,2014-12-18 01:00:00.0] 14.724744437763079
[2014-11-19 01:00:00.0,2014-11-20 01:00:00.0] 14.7292351008916
[2016-01-12 01:00:00.0,2016-01-13 01:00:00.0] 14.736736736736736
[2014-01-29 01:00:00.0,2014-01-30 01:00:00.0] 14.754649782350613
[2014-12-09 01:00:00.0,2014-12-10 01:00:00.0] 14.896643199673504
[2015-04-01 02:00:00.0,2015-04-02 02:00:00.0] 14.904666888371578
[2016-02-04 01:00:00.0,2016-02-05 01:00:00.0] 14.922149570903146
[2015-01-08 01:00:00.0,2015-01-09 01:00:00.0] 15.02690763052209
[2015-11-25 01:00:00.0,2015-11-26 01:00:00.0] 15.045336671733882
[2015-01-25 01:00:00.0,2015-01-26 01:00:00.0] 15.064186310707186
[2014-11-26 01:00:00.0,2014-11-27 01:00:00.0] 15.132970792035787
[2014-12-03 01:00:00.0,2014-12-04 01:00:00.0] 15.198992443324936
[2014-12-08 01:00:00.0,2014-12-09 01:00:00.0] 15.272406794145924
[2015-01-29 01:00:00.0,2015-01-30 01:00:00.0] 15.316542802091934
[2015-01-12 01:00:00.0,2015-01-13 01:00:00.0] 15.405976792242887
[2015-12-02 01:00:00.0,2015-12-03 01:00:00.0] 15.420628358825962
[2015-01-27 01:00:00.0,2015-01-28 01:00:00.0] 15.524356000488341
[2014-10-22 02:00:00.0,2014-10-23 02:00:00.0] 15.532475035074688
[2014-11-17 01:00:00.0,2014-11-18 01:00:00.0] 15.542098840756559
[2013-12-31 01:00:00.0,2014-01-01 01:00:00.0] 15.56140350877193
[2015-02-23 01:00:00.0,2015-02-24 01:00:00.0] 15.621009098428454
[2016-01-05 01:00:00.0,2016-01-06 01:00:00.0] 15.652786190187765
[2014-11-05 01:00:00.0,2014-11-06 01:00:00.0] 15.825906313645621
[2015-12-09 01:00:00.0,2015-12-10 01:00:00.0] 15.833725374889738
[2015-03-04 01:00:00.0,2015-03-05 01:00:00.0] 15.8951498979752
[2016-06-07 02:00:00.0,2016-06-08 02:00:00.0] 15.900240661985297
[2015-12-01 01:00:00.0,2015-12-02 01:00:00.0] 15.902966066880289
[2015-11-04 01:00:00.0,2015-11-05 01:00:00.0] 15.942935630892178
[2016-02-23 01:00:00.0,2016-02-24 01:00:00.0] 16.077539341917024
[2016-02-22 01:00:00.0,2016-02-23 01:00:00.0] 16.090002960623703
[2016-02-05 01:00:00.0,2016-02-06 01:00:00.0] 16.13630105553006
[2014-12-10 01:00:00.0,2014-12-11 01:00:00.0] 16.201000732243106
[2016-01-26 01:00:00.0,2016-01-27 01:00:00.0] 16.241422196210404
[2014-12-16 01:00:00.0,2014-12-17 01:00:00.0] 16.331094615293978
[2014-01-20 01:00:00.0,2014-01-21 01:00:00.0] 16.354676500167617
[2016-01-11 01:00:00.0,2016-01-12 01:00:00.0] 16.36250402662944
[2014-01-24 01:00:00.0,2014-01-25 01:00:00.0] 16.39921040408732
[2015-02-10 01:00:00.0,2015-02-11 01:00:00.0] 16.430395286029952
[2016-02-08 01:00:00.0,2016-02-09 01:00:00.0] 16.436298736957717
[2015-02-12 01:00:00.0,2015-02-13 01:00:00.0] 16.45596946162959
[2016-01-20 01:00:00.0,2016-01-21 01:00:00.0] 16.457902358295406
[2016-02-01 01:00:00.0,2016-02-02 01:00:00.0] 16.476530332143838
[2016-02-15 01:00:00.0,2016-02-16 01:00:00.0] 16.484615384615385
[2016-01-28 01:00:00.0,2016-01-29 01:00:00.0] 16.633091962346125
```

###### TODO
1) Use Typed approach wherever possible
2) Add unit tests
3) Upgrade Spark version
4) New query idea: Average number of bookings at a particular station at any given hour