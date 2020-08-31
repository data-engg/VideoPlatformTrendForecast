Project Requirement

A video on-demand company wants to expand its customer base with an aim to enhance revenue. The company turns to big data to analyse huge amount of data collected from mobile app as well as website – pertaining to user behaviour. The analysis of data will aid the company in:
1.	Keeping track of user behaviour so that the user is shown more personalized content which will encourage the user to opt for paid services
2.	Calculation of royalty for videos so the creators are suitably rewarded which will encourage the creators to create quality content





Goals

The project aims to create a big data pipeline that collects the data (streaming and static), integrate the data from different sources in a unified format and present the data in a simple format to business stack





Architecture
 

The architecture implements lambda architecture. The various layers are:
1.	Speed layer: Real time data is generated in real time from mobile logs in json format. The events land in a spooling directory from there a flume agent forwards it to a Kafka topic. A spark streaming application validated the data, enriches it and ingest it to a hdfs based data lake.  
2.	Batch layer: Every two hours, two files arrive in the inbound path – xml and csv. A spark application will run a batch to validate data, enrich it and ingest it to data lake
3.	Serving layer: The data from batch and speed layer in ingested to lake via a hive staging table which partitions the data based on processing time. New data is moved from staging to main tables where a second spark application analyses it and presents the final analysis in hive tables. The business users can query these table to make informed decision.




Data format

All the three main datasets wiz json, csv and xml files, contain the below fields

User_id : Unique identifier of every user 
Video_id :	Unique identifier of every video 
Creator_id :	Unique identifier of the lead creator of the video 
Timestamp :	Timestamp when the video was generated 
minutes_played :	Minutes between video started and video stopped/paused 
Geo_cd :	'A' -> America region, 'AP' -> Asia Pacific region, 'J' -> Japan region, 'E' –> Europe, 'AU' -> Australia region 
Channel_id :	Unique identifier of the channel from where the video was played 
Video_end_type :	How the video was terminated. 0 - completed successfully, 1 - video was skipped, 2 - video was paused, 3 - other types of failure like device issue, network error, etc. 
Liked :	true - video was liked (like button was clicked), false - video was not liked ( like button was not clicked) 
Disliked :	true means video was disliked (dislike button was clicked), false means video was not disliked (dislike button was not clicked) 

There are lookup files which:
1.	Contains the details of channel_id along with the respective geo_cd 
2.	Contains the details of the creator's id 
3.	Contains the details of subscribers 
4.	Contains the details of artist_id along with royalty associated with each play of the video 




Technology stack

1.	Python: A simple python application is used to simulate the mobile app data. This code outputs a json document containing 10 records at a time.
2.	Flume: A flume agent monitors a spooling directory where the python app outputs json document. The records are forwarded to a Kafka topic. After forwarding, the json document, the file is deleted, to make room for the next file
3.	Kafka: The records are buffered in a Kafka topic which has a single partition and three replicas
4.	Cassandra: The lookup files are loaded in Cassandra tables which reduces latency while fetching lookup data
5.	Spark Streaming: A Spark Streaming application polls the Kafka topic every 5 minutes, processes the data and outputs it to a hive table
6.	Spark SQL: Spark SQL applications process the csv and xml and load it to a hive table. Also, data in hive table is analysed using Spark SQL.
7.	Hive: Data from streaming and SQL jobs are ingested to hdfs based data lake via hive tables. It unifies the data, organises the data according to processing time and does a second round of validation on datatypes before ingesting data. Also, results are published in hive tables so the business users can query the data stored in hdfs.
8.	Oozie: All the elements are well coordinated, and the process automated via oozie workflow and coordinator




Data analysis and presentation
1.	Most popular channel computed by counting the total views and likes. View is given first preference and likes second preference
2.	Total duration of videos viewed by subscribed users and unsubscribed users
3.	Determine the most popular creators. Most popular creators are those whose contents are viewed by unique users
4.	Determine videos generating maximum royalty. Videos having likes and completed successfully can only be considered for royalty.
5.	Find out the unsubscribed user who watched video for longest duration
