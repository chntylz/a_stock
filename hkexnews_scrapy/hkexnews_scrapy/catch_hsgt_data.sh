#!/bin/sh

max=375
for((i=0;i<$max;i++))
do
    input=$[max-i]
    #echo $input

    #get i day ago(format)
    target_day=`date -d "$input day ago" +"%Y%m%d"`
    target_file="./json/"$target_day".json"
    target_file_gz="./json/"$target_day".json.gz"

    #caculate the week day which is used to judge where stock market is open or not.
    weekday=`date -d $target_day +%w`
    echo "$target_day is $weekday"
    if [[ $weekday -eq 0 || $weekday -eq 6 ]];then
        echo "Sunday or Saturday"
    else
        echo "workday"
        echo $target_file_gz

        if [ -f $target_file ]; then
            tar czf $target_file_gz $target_file 
            rm $target_file
        else
            echo '$target_file is not exist'
        fi

        if [ ! -f $target_file_gz  ]; then
            echo 'scrapy crawl hkexnews -a date='$target_day  '-o '$target_file
            scrapy crawl hkexnews -a date=$target_day -o $target_file
            tar czf $target_file_gz $target_file 
            rm $target_file

        fi
        
    fi
    
    

done



#    python test_cross_macd.py 17
#    python test_cross_macd.py 16
#    python test_cross_macd.py 15
#    python test_cross_macd.py 14
#    python test_cross_macd.py 13

#    python test_cross_macd.py 10
#    python test_cross_macd.py 9
#    python test_cross_macd.py 8
#    python test_cross_macd.py 7
#    python test_cross_macd.py 6

#    python test_cross_macd.py 3
#    python test_cross_macd.py 2
#    python test_cross_macd.py 1
