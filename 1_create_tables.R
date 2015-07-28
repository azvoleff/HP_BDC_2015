Sys.setenv(VERTICAINI="/home/team/vRODBC/vertica.ini")
Sys.setenv(ODBCINI="/home/team/vRODBC/odbc.ini")

library(vRODBC)

sites <- read.csv('sitecode_key.csv')

con <- odbcConnect("ctf")

for (site_num in 1:nrow(sites)) {
    sitecode <- sites[site_num, ]$sitecode
    pred_pix <- sites[site_num, ]$pred_pix
    train_pix <- sites[site_num, ]$train_pix
    sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "_predictor;"))
    create_pred_str <- paste0("CREATE TABLE cit.", sitecode, "_predictor
           (
            rowid IDENTITY(0,1,", pred_pix, "),
            pixelid INT,
            pixel_coord VARCHAR(100),
            r1 INT,
            r2 INT,
            r3 INT,
            r4 INT,
            r5 INT,
            r7 INT,
            veg INT,
            vegmean INT,
            vegvar INT,
            vegdis INT,
            elev INT,
            slop INT,
            asp INT,
            datayear INT
           ) ORDER BY pixelid SEGMENTED BY HASH(pixelid) ALL NODES;")
    sqlQuery(con, create_pred_str)

    sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "_predictor_mask;"))
    create_mask_str <- paste0("CREATE TABLE cit.", sitecode, "_predictor_mask
           (
            rowid IDENTITY(0,1,", pred_pix, "),
            datayear INT,
            pixelid INT,
            pixel_coord VARCHAR(100),
            fill INT,
            fmask INT
           ) ORDER BY pixelid SEGMENTED BY HASH(pixelid) ALL NODES;")
    sqlQuery(con, create_mask_str)

    sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "_training;"))
    create_train_str <- paste0("CREATE TABLE cit.", sitecode, "_training
           (
            rowid IDENTITY(0,1,", train_pix, "),
            pixelid INT,
            type VARCHAR(50),
            r1 INT,
            r2 INT,
            r3 INT,
            r4 INT,
            r5 INT,
            r7 INT,
            veg INT,
            vegmean INT,
            vegvar INT,
            vegdis INT,
            elev INT,
            slop INT,
            asp INT,
            datayear INT
           ) ORDER BY pixelid SEGMENTED BY HASH(pixelid) ALL NODES;")
    sqlQuery(con, create_train_str)
}

# Create spatial ref table

sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "spatial_ref;"))
create_sr_str <- "CREATE TABLE cit.spatial_ref
(
 sitecode VARCHAR(10),
 datayear INT,
 imgtype VARCHAR(20),
 nrows INT,
 ncols INT,
 xmn INT,
 xmx INT,
 ymn INT,
 ymx INT,
 nl INT,
 proj4string VARCHAR(150)
);"
sqlQuery(con, create_sr_str)

sqlQuery(con, paste0("DROP TABLE cit.", sitecode, "teamsites;"))
create_ts_str <- "CREATE TABLE cit.teamsites
(
 site_id INT,
 site_name VARCHAR(100),
 last_event_at DATETIME,
 last_event_by INT,
 time_zone VARCHAR(10),
 site_institution_name VARCHAR(100),
 site_abbv VARCHAR(5),
 site_status VARCHAR(15),
 country_id INT,
 short_name VARCHAR(50),
 latitude DOUBLE precision,
 longitude DOUBLE precision,
 start_date DATETIME,
 version_id INT
);"
sqlQuery(con, create_ts_str)