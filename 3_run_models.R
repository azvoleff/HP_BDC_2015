#Sys.setenv(VERTICAINI="/home/team/vRODBC/vertica.ini")
#Sys.setenv(ODBCINI="/home/team/vRODBC/odbc.ini")
Sys.setenv(VERTICAINI="/home/radmin/vRODBC/vertica.ini")
Sys.setenv(ODBCINI="/home/radmin/vRODBC/odbc.ini")

library(vRODBC)
library(distributedR)
library(HPdclassifier) # for hpdrandomForest
library(HPdata) # for db2dframe

pred_cols <- list("r1", "r2", "r3", "r4", "r5", "r7", "veg", "vegmean", 
                  "vegvar", "vegdis", "elev", "slop", "asp", "datayear")

distributedR_start(cluster_conf='/opt/hp/distributedR/conf/cluster_conf.xml')
#distributedR_start(cluster_conf='/home/team/cluster_conf_1.xml')

#sites <- read.csv('/home/team/HP_BDC_2015/sitecode_key.csv')
sites <- read.csv('/home/radmin/HP_BDC_2015/sitecode_key.csv')
sitecodes <- sites$sitecode

sitecode <- 'PSH'

foreach (sitecode=sitecodes) %do% {
    tr_sql <- paste("SELECT", paste(c(pred_cols, "type"), collapse=", "), "FROM cit.PSH_training;")

    # Below doesn't work, so use with verticaConnector=FALSE
    #train_data <- db2dframe(paste0("cit.", sitecode, "_training"), 'ctf',
    #                        features=c(pred_cols, "type"))

    train_data <- db2dframe(paste0("cit.", sitecode, "_training"), 'ctf',
                            features=c(pred_cols, "type"), verticaConnector=FALSE)

    # Ensure NA codes are handled properly
    train_data[train_data == -32768] <- NA

    message(date(), ": Training randomForest model")
    rfmodel <- hpdrandomForest(type ~ ., data=train_data, na.action=na.omit, ntree=2000)
    message(date(), ": finished training randomForest model")

    # Use deploy.model around here...
    deploy.model(model=rfmodel, dsn='ctf', modelName=paste0(sitecode, '_rfmodel'),
                 modelComments=paste('Random forest model for', sitecode))


    con <- odbcConnect("ctf")
    apply_sql <- paste0("CREATE TABLE ", sitecode, "_prediction", " AS
                        (SELECT ", paste(pred_cols, collapse=", "),
                        " USING PARAMETERS model='dbadmin/", sitecode, "'",
                        " TYPE='response')")
    sqlQuery(con, apply_sql)

    # Set missing pixels in result to NA using the mask

    message(date(), ": Started loading data as dframe")
    indep_data <- db2dframe(paste0("cit.", sitecode, "_predictor"), 'ctf',
                            features=pred_cols, verticaConnector=FALSE)
    message(date(), ": Finished loading dframe")

    message(date(), ": Recoding NAs in dframe")
    # Recode NAs in the independent variables used for the predictions
    foreach(i, 1:npartitions(indep_data),
        code_NAs <- function(x=splits(indep_data, i)) {
        x[x == -32768] <- NA
        update(x)
        }
    )
    message(date(), ": Finished recoding NAs in dframe")

    message(date(), ": Started predicting from randomForest model")
    res <- predict.hpdRF_parallelTree(rfmodel, newdata=indep_data)
    message(date(), ": finished predicting from randomForest model")
}
