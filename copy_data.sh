if [ $# -ne 1 ]; then
echo "Usage: create_tables.sh password"
exit
fi

pword=$1

# List predictor files in Mosaics folder
files=( $( ls /home/radmin/Mosaics | grep predictors.txt ) )

for i in "${files[@]}"
do
    dy=$( echo $i | grep -oP "\K\d{4}")
    sitecode=$( echo $i | grep -oP "\K^[A-Z]{2,3}")

    echo Started Copy Year ${dy} ${sitecode} Predictor at `date`
    /opt/vertica/bin/vsql -h 10.0.0.32 -U dbadmin -w $pword -c " \
    COPY cit.${sitecode}_predictor( \
    pixelid, \
    pixel_coord,
    r1, r2, r3, r4, r5, r7, veg, vegmean, vegvar, vegdis, elev, slop, asp, \
    datayear AS ${dy} \
    ) FROM LOCAL '/home/radmin/Mosaics/${sitecode}_mosaic_${dy}_predictors.txt' DELIMITER AS '|' REJECTED DATA './logs/${sitecode}_${dy}_p_rejected.dat' EXCEPTIONS './logs/${sitecode}_${dy}_p_exceptions.log'; "
    echo Done Copy Year ${dy} ${sitecode} Predictor at `date`

    echo Started Copy Year ${dy} ${sitecode} Predictor Mask  at `date`
    /opt/vertica/bin/vsql -h 10.0.0.32 -U dbadmin -w $pword -c " \
    COPY cit.${sitecode}_predictor_mask( \
    datayear AS ${dy}, \
    pixelid, \
    pixel_coord,
    fill, \
    fmask \
    ) FROM LOCAL '/home/radmin/Mosaics/${sitecode}_mosaic_${dy}_predictors_masks.txt' DELIMITER AS '|' REJECTED DATA './logs/${sitecode}_${dy}_pm_rejected.dat' EXCEPTIONS './logs/${sitecode}_${dy}_pm_exceptions.log'; "
    echo Done Copy Year ${dy} ${sitecode} Predictor Mask  at `date`
done

sitecodes=( $( ls /home/radmin/Mosaics | grep -oP "\K^[A-Z]{2,3}" | uniq ) )

for sitecode in "${sitecodes[@]}"
do
    echo Started Copy Year ${sitecode} Training at `date`
    /opt/vertica/bin/vsql -h 10.0.0.32 -U dbadmin -w $pword -c " \
    COPY cit.${sitecode}_training( \
    pixelid, \
    type,
    r1, r2, r3, r4, r5, r7, veg, vegmean, vegvar, vegdis, elev, slop, asp, datayear \
    ) FROM LOCAL '/home/radmin/Training/${sitecode}_training.txt' DELIMITER AS '|' REJECTED DATA './logs/${sitecode}_t_rejected.dat' EXCEPTIONS './logs/${sitecode}_t_exceptions.log'; "
    echo Done Copy Year ${sitecode} Training at `date`
done
