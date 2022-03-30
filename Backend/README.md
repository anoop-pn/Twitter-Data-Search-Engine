# Backend Code Usage

# Prerequisites
You will need Java 1.8 version installed on your system

## Once downloaded, open the terminal in the project directory, and continue with:
File src/main/java/com/mylucene/app/App.java has all the methods

## Arguments to be passed
1st Argument -> Path of tweetsData.json  ---> This is tweets data collected from crawler

2nd Argument -> Path of invertedIndex.csv  ---> This is invertedIndex csv file generated from hadoop for the tweets data

## Command to run
### java src/main/java/com/mylucene/app/App.java tweetsData.json invertedIndex.csv --server.port=8083

This will run the backend on the port 8083 and you'll be able to access from browser using link: http://localhost:8083/

## Packing for production
Run the following command from backend root folder my-luceneApp 
### mvn clean install -Dspring.profiles.active=dev

This will build the jar in the folder "my-luceneApp\target\my-luceneApp-1.0-SNAPSHOT.jar"

Now you can use this jar to run the backend using command: 
### java -jar my-luceneApp-1.0-SNAPSHOT.jar tweetsData.json invertedIndex.csv --server.port=8083

This will run the backend on the port 8083 and you'll be able to access from browser using link: http://localhost:8083/
