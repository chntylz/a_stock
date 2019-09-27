#!/bin/sh

#check whether date is valid, or not
function is_holiday() 
{
    arr=('20190913','20190914')
    sub_str=$1
    echo "target_day is $sub_str " 

    if [[ " ${arr[*]} " == *"$sub_str"* ]]; then
        echo "arr contains $sub_str" ;
        return 1
    else
        echo "arr not contains $sub_str" 
        return 0
    fi
}


max=365
for((i=0;i<$max;i++))
do
    input=$[max-i]
    #echo $input

    #get i day ago(format)
    target_day=`date -d "$input day ago" +"%Y%m%d"`
    is_holiday $target_day
    valid_chk=$?
    echo "valid_chk is $valid_chk"
    if [[ $valid_chk -eq 1 ]]; then
        echo "holiday"
        continue
    fi

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

        if [ ! -f $target_file_gz ]; then
            echo 'scrapy crawl hkexnews -a date='$target_day  '-o '$target_file
            scrapy crawl hkexnews -a date=$target_day -o $target_file

            tar czf $target_file_gz $target_file 
            echo 'tar czf' $target_file_gz $target_file 

            rm $target_file
            echo 'rm' $target_file
        fi

        #check file size
        file_size=`wc -c < $target_file_gz`
        if [[ $file_size -le 50000 ]] && [[ $max -eq $[i+1] ]] ; then
            echo "$i: $target_file_gz not valid, delete it"
            rm $target_file_gz
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
