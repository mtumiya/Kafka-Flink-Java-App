
To run this app ; make sure kafka is running in flink-kafka-project :: command>> docker compose run -d , 
to check if its running command>> docker compose ps 

After kafka starts, clean the java app by rooting into its directory in shell command>> mvn clean
then head to UserActivityProducer class and run the function. 

After this locate the java jar file that got created after the clean up and upload it into flink ui and run the job by submitting it. 

this will make it run.