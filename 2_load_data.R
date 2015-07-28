library(foreach)
library(stringr)

sites <- read.csv('sitecode_key.csv')

mosaics <- dir('/home/team/Mosaics', pattern='[A-Z]{2,3}_mosaic_[0-9]{4}_predictors.txt')

mosaics <- mosaics[!grepl('PSH', mosaics)]

sitecodes <- str_extract(mosaics, '^[A-Z]{2,3}')
years <- str_extract(mosaics, '[0-9]{4}')

# Load predictor data
foreach (sitecode=sitecodes, year=years) %do% {
    message(paste0("Loading data from ", sitecode, "..."))
    system(paste("/home/team/HP_BDC_2015/copy_data.sh", year, sitecode))
    message(paste0("Finished loading data from ", sitecode, "."))
}

# Load training data
foreach (sitecode=unique(sitecodes)) %do% {
    message(paste0("Loading training data from ", sitecode, "..."))
    system(paste("/home/team/HP_BDC_2015/copy_training.sh", sitecode))
    message(paste0("Finished loading training data from ", sitecode, "."))
}
