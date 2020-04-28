# spring-spark-analyse
## description
These Application provides several Webservices to analyse a huge amount of csv-files, which are collecting natural catastrophes. A csv-file has the following structure:

Date	    Catastrophe	DamageAmount

20200802	Flood	      3000000.00

20200805	Earthquake	3000000.00

20200807	Flood	      29000000.00

20200821	Windstorm	  4300000.00

20200823	Hurricane	  230000.00

## build and run
mvnw spring-boot:run
## REST services
Please call these REST services via Webclient (i. e. Postman) and choose POST requests

Search in all csv-files the pattern and gives the concerned file with the count back:

http://localhost:8080/analyseFilesCount?pattern=Hurricane

Adds all damage amounts from all csv-files with the given pattern and gives back the sum:

http://localhost:8080/analyseAmount?pattern=Hurricane

Returns a list of dates, when the catastrophe happens:

http://localhost:8080/analyseDate?pattern=Hurricane

Example to find a pattern in a very big file (6,5 MB) to retrieve it via Spark or pure old java:

http://localhost:8080/wordcountSpark?pattern=John

http://localhost:8080/wordcountLegacy?pattern=John

A first try in machine learning, given back a statistic table of a csv-file of double values:

http://localhost:8080/statistics
