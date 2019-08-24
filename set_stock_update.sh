#/bin/sh
#20190625,by aaron

export PATH="/home/ubuntu/anaconda2/bin:$PATH"
source activate python3.5
cd /home/ubuntu/a_stock
mkdir -p runlog
logfile="/home/ubuntu/a_stock/runlog/update.log"

echo "#####################################################" >>  $logfile
echo "" >> $logfile


file_array=(
            'main_day.py' 
            'test_read_db_data.py' 
            'test_generate_html.py' 
            'test_generate_index_html.py'
            'test_cross_macd.py'
            'test_generate_macd_html.py'
            'test_generate_macd_index_html.py'
            )

for value in ${file_array[@]}
do
    echo "" >> $logfile
    target=$value
    echo $target >> $logfile

    if [ "$target" = "test_generate_index_html.py" ];then
        echo "cd /var/www/html/"  >> $logfile
        cd /var/www/html/
    elif [ "$target" = "test_generate_macd_index_html.py" ];then
        echo "cd /var/www/html/"  >> $logfile
        cd /var/www/html/
        rm macd-index.html
    else
        echo "cd /home/ubuntu/a_stock/"  >> $logfile
        cd /home/ubuntu/a_stock
    fi

    time=`date "+%Y_%m_%d_%H_%M_%S"`
    echo "time python $target start $time" >> $logfile
    tt_1=`date "+ %s"`
    time python $target 0 
    tt_2=`date "+ %s"`
    time=`date "+%Y_%m_%d_%H_%M_%S"`
    echo "time python $target stop $time, cost time is $(($tt_2 - $tt_1))" >> $logfile
done

echo "#####################################################" >>  $logfile

exit


#delete the follow
#####################################################################################3

