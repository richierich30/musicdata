# MusicData.txt
MapReduce Job for musicdata.txt (https://docs.google.com/document/d/1l2C7kTsI9HBLFAxK98PDkxqZUwcF2eU7daNudAW85zA/edit)
Dataset is sample data of songs heard by users on an online streaming platform. The Description of data set attached in musicdata.txt is as follows: -
1st Column - UserId
2nd Column - TrackId
3rd Column - Songs Share status (1 for shared, 0 for not shared)
4th Column - Listening Platform (Radio or Web - 0 for radio, 1 for web)
5th Column - Song Listening Status (0 for skipped, 1 for fully heard)


## Task 1
Find the number of unique listeners in the data set.

### Solution-
The UniqueListener Job calculates number of Unique Listener per track.
Map:-
Map processes the space "|" delimeter , fetches listening data and produces the UserId associated with the TrackId.
Reduce:-
Reduce receives a list of UserIds per TrackId and put these Ids in a set to remove any duplicates. The size of this set is then calculated for each TrackId.
First I checked if all the hadoop daemons were running.
I found History Server was not running , so I started it using command "mr-jobhistory-daemon.sh start historyserver"
Then I checked for my jar file. It was in the Desktop "my_first.jar".
Then I saved my sample data for job i.e. "musicdata.txt" into hdfs.
I executed my job with command "hadoop jar /home/acadgild/Desktop/my_first.jar /musicdata.txt /sample_out".
When the job completed successfully,I checked the sample_out with command "hadoop dfs -ls /sample_out"
It showed 2 files namely SUCCESS and part-r-00000.
I checked the result stored in part-r-00000 using "hadoop dfs -cat /sample_out/part-r-00000".

## OUTPUT-
It showed the result as-
222        1
223        1
225        5


## Task 2
What are the number of times a song was heard fully.

### Solution-
Map:-
Map fetches the TrackId of each song and the status of Is_Skipped which give 1 if song is fully heard by the user otherwise 0.
Reduce:-
Reduce receives Is_Skipped status per TrackId , we checked with the help of if statement whether Is_Skipped == 1 and if it does then the count is increament by 1.
Combiner:-
Combiner is used to combine  TrackId so that there will be no ambiguity in program.
I executed my job with command "hadoop jar /home/acadgild/Desktop/my_second.jar /musicdata.txt /out2".
When the job completed successfully,I checked the sample_out with command "hadoop dfs -ls /out2"
It showed 2 files namely SUCCESS and part-r-00000.
I checked the result stored in part-r-00000 using "hadoop dfs -cat /out2/part-r-00000".

### OUTPUT-
It showed the result as-
222        0
223        1
225        2


## Task 3
What are the number of times a song was shared.

### Solution-
Map:-
Map fetches the TrackId of each song and the status of Is_Shared which give 1 if song is shared by the user otherwise 0.
Reduce:-
Reduce receives Is_Shared status per TrackId , we checked with the help of if statement whether (Is_Shared == 1) and if it does then the count is increament by 1.
I executed my job with command "hadoop jar /home/acadgild/Desktop/my_third.jar /musicdata.txt /out5".
When the job completed successfully,I checked the sample_out with command "hadoop dfs -ls /out5"
It showed 2 files namely SUCCESS and part-r-00000.
I checked the result stored in part-r-00000 using "hadoop dfs -cat /out5/part-r-00000".

## OUTPUT-
It showed the result as-
222        0
223        0
225        2
 
