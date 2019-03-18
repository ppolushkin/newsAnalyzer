#News Analyzer
Application that analyzes news for a given day. It's a small project to practice microsoft azure batch processing.

## News API
https://newsapi.org

## Build project 
``mvn clean build``

### Install batch service
```
az group create --name batch-rg --location "France Central"
az group deployment create \
  --name batch-deployment \
  --resource-group batch-rg \
  --template-file azure/azuredeploy.json
```
Then copy newsAnalyzer-1.zip as batch account application. It should be named as newsAnalyzer with version 1.

### Run batch tasks
- Copy new variables into AzureBatchStarter.java
- Run AzureBatchStarter from IDEA
- This is how Azure batch will prepare Linux node and start application
```
bash -c "apt install openjdk-8-jre-headless -y && apt install language-pack-ru -y && update-locale LANG=ru_RU.UTF-8"
bash -c "java -jar $AZ_BATCH_APP_PACKAGE_newsanalyzer_1/newsAnalyzer-1.jar --date=2019-02-20"
```