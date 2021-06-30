cp -rf ../file_interface.py .
cp -rf ../HData_day.py      .
cp -rf ../HData_hsgt.py     .
cp -rf ../zig.py            .
cp -rf ../comm_generate_html.py .
cp ../eastmoney/get_daily_zlje.py .
cp ~/eastmoney/get_realtime_data.py .
cp ../eastmoney/HData_eastmoney_zlje*.py .
cp ../eastmoney/HData_eastmoney_fund.py .
cp ../pysnow_ball/HData_xq_fina.py .
cp ../pysnow_ball/HData_xq_holder.py .
cp ../pysnow_ball/test_get_basic_data.py .
cp ../pysnow_ball/comm_interface.py .

#rm    file_interface.py
#rm    HData_day.py     
#rm    HData_hsgt.py    
#rm    zig.py           
#rm    comm_generate_html.py

cp -rf *.py /var/www/cgi-bin/
cp -rf *.cgi /var/www/cgi-bin/
cp -rf *.txt /var/www/cgi-bin/
