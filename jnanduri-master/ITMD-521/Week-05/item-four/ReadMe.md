JAY-WEEK_05 MAP REDUCE
------------------------------------

- SOUTHERN HEMISPHERE-maximum temperature per 10 degree latitute(output--part-r-00000)

OUTPUT DESCRIPTION :
[ex: 0-10 362
latitude (#between0 to 10)/1000 temperature/10(#in celsius)]

![output](https://user-images.githubusercontent.com/31421387/31051146-3bc9cac0-a626-11e7-87dd-d16397e82cb5.png)


-  SOUTHERN HEMISPHERE-maximum temperature per 10 degree latitute(Job History-19888)

![job history](https://user-images.githubusercontent.com/31421387/31051147-40879826-a626-11e7-8fde-199937269305.png)


-INSTRUCTIONS 

1.Compile the java file 
2.Create a jar file 
3.Give input as arguments
4.Run the code using hadoop -cat 


-THOUGHT PROCESS

I considered the latitute as the key and used switch case to group the latitute and emitted the latitute and temperature as the key value pair to the reducer to emit out the maximum temperature per degree latitute.
