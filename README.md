#News Analyzer
Application that analyzes news for a given day. It's a small project to practice microsoft azure batch processing.

## News API
https://newsapi.org

## Build project 
``mvn clean build``

## Run application  
``java -jar newsAnalizer-1.0.jar -Dspring.config.location=application.yml > logs.txt 2>&1``

sudo chmod +x prod/run.sh