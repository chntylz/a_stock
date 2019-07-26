# a_stock

#set_stock_update.sh


#/bin/sh
#20190625,by aaron

export PATH="/home/ubuntu/anaconda2/bin:$PATH"
source activate python3.5
cd /home/ubuntu/a_stock
mkdir -p runlog
logfile="/home/ubuntu/a_stock/runlog/update.log"

echo "#####################################################" >>  $logfile
echo "" >> $logfile

#main_day.py
target="main_day.py"
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target start $time" >> $logfile
tt_1=`date "+ %s"`
time python $target
tt_2=`date "+ %s"`
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target stop $time, cost time is $(($tt_2 - $tt_1))" >> $logfile


echo "" >> $logfile

#test_read_db_data.py
target="test_read_db_data.py"
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target start $time" >> $logfile
tt_1=`date "+ %s"`
time python $target
tt_2=`date "+ %s"`
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target stop $time, cost time is $(($tt_2 - $tt_1))" >> $logfile

echo "" >> $logfile

#test_generate_html.py
target="test_generate_html.py"
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target start $time" >> $logfile
tt_1=`date "+ %s"`
time python $target
tt_2=`date "+ %s"`
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target stop $time, cost time is $(($tt_2 - $tt_1))" >> $logfile

echo "" >> $logfile


cd /var/www/html/
#test_generate_index_html.py
target="test_generate_index_html.py"
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target start $time" >> $logfile
tt_1=`date "+ %s"`
time python $target
tt_2=`date "+ %s"`
time=`date "+%Y_%m_%d_%H_%M_%S"`
echo "time python $target stop $time, cost time is $(($tt_2 - $tt_1))" >> $logfile

echo "" >> $logfile

echo "#####################################################" >>  $logfile


