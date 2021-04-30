#!/bin/sh

#check whether date is valid, or not
function is_holiday() 
{
    #arr=('20190913','20190914', '20191002','20191003','20191004', '20191005', '20200101','20200124', '20200127', '20200128', '20200129', '20200130', '20200131', '20200406')
    #arr=('20190913','20190914', '20191002','20191003','20191004', '20191005','20191007', '20200101', '20200124', '20200127', '20200128', '20200129', '20200130', '20200131', '20200406','20200409', '20200410','20200413','20200430', '20200501', '20200504', '20200505', '20200625', '20200626', '20200701', '20201001', '20201002','20201001','20201005','20201006','20201007','20201008', '20201013', '20201026', '20201225')
    arr=('20190913','20190914', '20191002','20191003','20191004', 
    '20191005', '20191007', '20200101', '20200124', '20200127', 
    '20200128', '20200129', '20200130', '20200131', '20200406', 
    '20200410', '20200413', '20200430', '20200501', '20200504', 
    '20200505', '20200625', '20200626', '20200701', '20201001', 
    '20201002', '20201001', '20201005', '20201006', '20201007',
    '20201008', '20201013', '20201026', '20201224', '20201225',
    '20210101', 
    '20210209',
    '20210211', '20210212', '20210215','20210216',
    '20210217',
    '20210402', '20210405', 
    '20210430',
    '20210503', '20210504', '20210505',
    '20210519',
    '20210614',
    '20210701', '20210916', '20210917',  
    '20210920', '20210921', '20210922', '20210929', '20210930',
    '20211001', '20211004', '20211005', '20211006', '20211007',
    '20211014', '20211224', '20211225'
    )
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
        echo "holiday, skip this day !!!"
        continue
    fi

    target_file="./json/"$target_day".json"
    target_file_gz="./json/"$target_day".json.gz"

    #caculate the week day which is used to judge where stock market is open or not.
    weekday=`date -d $target_day +%w`
    echo "$target_day is $weekday"
    if [[ $weekday -eq 0 || $weekday -eq 6 ]];then
        echo "Sunday or Saturday, skip this day!!!"
    else
        echo "workday"
        echo $target_file_gz

        if [ ! -f $target_file_gz ]; then
            echo 'scrapy crawl hkexnews -a date='$target_day  '-o '$target_file
            scrapy crawl hkexnews -a date=$target_day -o $target_file
            #echo 'scrapy crawl hkexnews_oneday -a date='$target_day  '-o '$target_file
            #scrapy crawl hkexnews_oneday -a date=$target_day -o $target_file

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
