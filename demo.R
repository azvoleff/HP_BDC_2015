library(vRODBC) # for SQL connection to Vertica database
library(distributedR) # for hpdRF_parallelTree, deploy.model
library(HPdclassifier) # for hpdrandomForest
library(HPdata) # for db2dframe

pred_cols <- list("r1", "r2", "r3", "r4", "r5", "r7", "veg", "vegmean", 
                  "vegvar", "vegdis", "elev", "slop", "asp", "datayear")

distributedR_start()
con <- odbcConnect("ctf")

message(date(), ": Training random forest model")
train_data <- db2dframe("cit.PSH_training", 'ctf', features=c(pred_cols, "type"))

rfmodel <- hpdRF_parallelTree(type ~ ., data=train_data, na.action=na.omit,
                              ntree=1000)
message(date(), ": Finished training random forest model")

message(date(), ": Running in-database prediction in HP Vertica")

# Deploy model to Vertica DB
sqlQuery(con, "DELETE FROM R_models WHERE model = (SELECT DeleteModel(USING PARAMETERS model = 'dbadmin/PSH_rfmodel') over());")

deploy.model(model=model, dsn='ctf', modelName='PSH_rfmodel', modelComments='Random forest model for Pasoh')

# Create table to hold predictions
sqlQuery(con, 'DROP TABLE cit.PSH_prediction;')
sqlQuery(con, 'CREATE TABLE cit.PSH_prediction (type varchar);')

# Run in-database prediction
sqlQuery(con, paste0("INSERT INTO cit.PSH_prediction (SELECT randomForestpredict(",
                     paste(pred_cols, collapse=", "),
                     " USING PARAMETERS model='PSH_rfmodel') FROM cit.PSH_predictor WHERE datayear = 2010);")

message(date(), ": Finished running in-database prediction in HP Vertica")
