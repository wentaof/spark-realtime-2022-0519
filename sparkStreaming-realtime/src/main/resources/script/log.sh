#!/bin.bash
dirpath="/opt/module/applog"
cd $dirpath
if [ $# > 1 ]
then
    sed -i '/mock.date:/c mock.date: ${1}/' application.yml
    nohup java -jar gmall2020-mock-log-2021-11-29.jar > /etc/null 2>&1 &
else
    echo "usage: log.sh yyyy-mm-dd;"
fi