hdfs dfs -rm -r /Users/nikitamorozov/edu_itmo/paralells/lab4-netos23/wd
hdfs dfs -mkdir -p  /Users/nikitamorozov/edu_itmo/paralells/lab4-netos23/wd/input
hdfs dfs -put -f ./assets/*.csv /Users/nikitamorozov/edu_itmo/paralells/lab4-netos23/wd/input
hadoop jar SalesJobs/build/libs/SalesJobs-1.0-SNAPSHOT.jar ru.fbtw.Runner ./wd/input ./wd/output "8" "16777216"
