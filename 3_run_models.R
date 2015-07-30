library(vRODBC)
library(distributedR)
library(HPdclassifier) # for hpdrandomForest
library(HPdata) # for db2dframe
library(foreach) # for db2dframe

pred_cols <- list("r1", "r2", "r3", "r4", "r5", "r7", "veg", "vegmean", 
                  "vegvar", "vegdis", "elev", "slop", "asp", "datayear")

distributedR_start()

sites <- read.csv('/home/radmin/HP_BDC_2015/sitecode_key.csv')
sitecodes <- sites$sitecode

foreach (sitecode=sitecodes) %do% {
    message(date(), ": Loading training data")
    train_data <- db2dframe(paste0("cit.", sitecode, "_training"), 'ctf',
                            features=c(pred_cols, "type"))
    message(date(), ": Finished loading training data")

    message(date(), ": Training random forest model")
    rfmodel <- hpdRF_parallelTree(type ~ ., data=train_data, na.action=na.omit,
                                 ntree=1000)
    message(date(), ": Finished training random forest model")

    message(date(), ": Deploying model to vertica db")
    deploy.model(model=rfmodel, dsn='ctf', modelName=paste0(sitecode, '_rfmodel'),
                modelComments=paste('Random forest model for', sitecode))
    message(date(), ": Finished deploying model to vertica db")

    # message(date(), ": Running in-database prediction in vertica")
    con <- odbcConnect("ctf")
    sqlQuery(con, paste0('DROP TABLE cit.', sitecode, '_prediction;'))
    sqlQuery(con, paste0('CREATE TABLE cit.', sitecode, '_prediction (type varchar);'))
    apply_sql <- paste0("INSERT INTO cit.", sitecode, "_prediction", 
                        " (SELECT randomForestpredict(",
                        paste(pred_cols, collapse=", "),
                        " USING PARAMETERS model='dbadmin/", sitecode,
                        "_rfmodel') FROM cit.", sitecode,
                        "_predictor WHERE datayear = 2010);")
    sqlQuery(con, apply_sql)
    message(date(), ": Finished running in-database prediction in vertica")
}
