-- HIVE properties
set hive.exec.dynamic.partition.mode=nonstrict;

-- select database
USE VIDEOS;

--Move data from stagging to main table
INSERT OVERWRITE  TABLE VIDEO_PLAY 
partition(process_time)
select t1.* from video_play_stg t1 
inner join (select last_updated_time from video_play_last_updated) t2
inner join (select last_updated_time from video_play_stg_last_updated) t3
where unix_timestamp(t1.process_time, 'dd/MM/yyyy HH:mm:ss') > t2.last_updated_time
and  unix_timestamp(t1.process_time, 'dd/MM/yyyy HH:mm:ss') <= t3.last_updated_time;

-- UPDATE video_play_last_updated
INSERT OVERWRITE TABLE VIDEO_PLAY_LAST_UPDATED
SELECT max(unix_timestamp(process_time, 'dd/MM/yyyy HH:mm:ss')) FROM VIDEO_PLAY;