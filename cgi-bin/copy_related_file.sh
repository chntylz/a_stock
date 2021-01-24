cp -rf ../file_interface.py .
cp -rf ../HData_day.py      .
cp -rf ../HData_hsgt.py     .
cp -rf ../zig.py            .
cp -rf ../comm_generate_html.py .
cp ../eastmoney/get_daily_fund.py .
cp ../eastmoney/HData_eastmoney_fund*.py .

#rm    file_interface.py
#rm    HData_day.py     
#rm    HData_hsgt.py    
#rm    zig.py           
#rm    comm_generate_html.py

cp -rf *.py /var/www/cgi-bin/
cp -rf *.cgi /var/www/cgi-bin/
cp -rf *.txt /var/www/cgi-bin/
