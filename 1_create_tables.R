library(vRODBC)

sites <- read.csv('sitecode_key.csv')

con <- odbcConnect("ctf")

message("Creating data tables...")
for (sitecode in sites$sitecode) {
    train_pix <- sites$train_pix[sites$sitecode == sitecode]
    pred_pix <- sites$pred_pix[sites$sitecode == sitecode]
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
           ) ORDER BY rowid SEGMENTED BY HASH(pixelid) ALL NODES;")
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
           ) ORDER BY rowid SEGMENTED BY HASH(pixelid) ALL NODES;")
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
           ) ORDER BY rowid SEGMENTED BY HASH(pixelid) ALL NODES;")
    sqlQuery(con, create_train_str)
}
message("Finished creating data tables.")

# Load spatial ref data
message("Loading image metadata...")
sqlQuery(con, "DROP TABLE cit.spatial_ref;")
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

metadata_sql <- "COPY cit.spatial_ref( \
 sitecode,
 datayear,
 imgtype,
 nrows,
 ncols,
 xmn,
 xmx,
 ymn,
 ymx,
 nl,
 proj4string 
) FROM LOCAL '/home/team/HP_BDC_2015/image_metadata.txt' DELIMITER AS '|'
REJECTED DATA './logs/image_metadata_rejected.dat' EXCEPTIONS './logs/image_metadata_exceptions.log'; "
sqlQuery(con, metadata_sql)
message("Finished loading image metadata.")

# Load site metadata
sqlQuery(con, "DROP TABLE cit.teamsites;")
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
