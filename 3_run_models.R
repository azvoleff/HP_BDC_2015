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

#distributedR_start(cluster_conf='/home/team/cluster_conf_1.xml')
distributedR_start(cluster_conf='/opt/hp/distributedR/conf/cluster_conf.xml')

#sites <- read.csv('/home/team/HP_BDC_2015/sitecode_key.csv')
sites <- read.csv('/home/radmin/HP_BDC_2015/sitecode_key.csv')
sitecodes <- sites$sitecode

sitecodes <- 'BBS'

foreach (sitecode=sitecodes) %do% {
    message(date(), ": Loading training data")
    train_data <- db2dframe(paste0("cit.", sitecode, "_training"), 'ctf',
                            features=c(pred_cols, "type"))
    message(date(), ": Finished loading training data")

    message(date(), ": Training random forest model")
    rfmodel <- hpdRF_parallelTree(type ~ ., data=train_data, na.action=na.omit,
                                  ntree=2000)
    message(date(), ": Finished training random forest model")

    # Below doesn't work:
    # deploy.model(model=rfmodel, dsn='ctf', modelName=paste0(sitecode, '_rfmodel'),
    #              modelComments=paste('Random forest model for', sitecode))
    
    # Below doesn't work because model won't deploy to vertica
    con <- odbcConnect("ctf")
    apply_sql <- paste0("CREATE TABLE ", sitecode, "_prediction", " AS
                        (SELECT ", paste(pred_cols, collapse=", "),
                        " USING PARAMETERS model='dbadmin/", sitecode, "'",
                        " TYPE='response')")
    sqlQuery(con, apply_sql)

    # message(date(), ": Started loading data as dframe")
    # indep_data <- db2dframe(paste0("cit.", sitecode, "_predictor"), 'ctf',
    #                         features=pred_cols, verticaConnector=FALSE)
    # message(date(), ": Finished loading data as dframe")

    # message(date(), ": Started applying random forest model")
    # res <- predict.hpdRF_parallelTree(rfmodel, newdata=indep_data)
    # message(date(), ": Finished applying random forest model")

    # message(date(), ": Saving results to vertica")
    # #TODO: Save results back to vertica
    # sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "_prediction;"))
    # create_pxn_str <- paste0("CREATE TABLE cit.", sitecode, "_prediction
    #        (
    #         rowid IDENTITY(0,1,", nrow(res), "),
    #         pixelid INT,
    #         type VARCHAR(50)
    #        ) ORDER BY pixelid SEGMENTED BY HASH(pixelid) ALL NODES;")
    # sqlQuery(con, create_pxn_str)
    # message(date(), ": Finished saving results to vertica")
}
