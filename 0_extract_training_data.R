library(stringr)
library(foreach)

input_dir <- 'O:/Data/Landsat/Composites/Models'
output_dir <- 'O:/Data/HP_BDC_2015'

training_files <- dir(input_dir, pattern='trainingpixels.RData')

foreach (training_file=training_files) %do% {
    sitecode <- str_extract(training_file, '^[A-Z]{2,3}')
    load(file.path(input_dir, training_file))
    training <- cbind(type=tr_pixels@y, tr_pixels@x)
    # Drop rows with NAs
    training <- training[complete.cases(training), ]
    training <- cbind(pixelid=0:(nrow(training) - 1), training)
    write.table(training, file=file.path(output_dir, paste0(sitecode, '_training.txt')),
                sep="|", row.names=FALSE, col.names=FALSE, quote=FALSE)
    message(paste(sitecode, nrow(training)))
}
