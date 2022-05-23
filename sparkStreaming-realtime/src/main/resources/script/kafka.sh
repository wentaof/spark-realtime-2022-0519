#!/bin/bash
#set -x
alert_str="Usage: kafka.sh {start|stop|kc [topic]|kp [topic]}|list|delete [topic]| describe [topic]|topic_list"
machines="root@192.168.198.132"
#source ~/.zshrc
if [ $# -lt 1 ]; then
    echo $alert_str
fi
case $1 in
start)
    for machine in $machines
    do
        ssh $machine "cd /root/app/kafka_2.11-0.11.0.3; bin/kafka-server-start.sh -daemon config/server.properties"
    done
;;

stop)
    for machine in $machines
    do
        ssh $machine "cd /root/app/kafka_2.11-0.11.0.3; bin/kafka-server-stop.sh"
    done
;;

kc)
    if [ $# -gt 1 ]; then
        kafka-console-consumer.sh --topic $2 --bootstrap-server 192.168.198.132:9092 --from-beginning
    else
        echo $alert_str
    fi
;;

kp)
    if [ $# -gt 1 ]; then
        kafka-console-producer.sh --topic $2 --broker-list 192.168.198.132:9092
    else
        echo $alert_str
    fi
;;

list)
    kafka-consumer-groups.sh --bootstrap-server 192.168.198.132:9092 --list
;;

describe)
    if [ $# -gt 1 ]; then
        kafka-consumer-groups.sh --bootstrap-server 192.168.198.132:9092 --describe --group $2
    else
        echo $alert_str
    fi
;;

delete)
    if [ $# -gt 1 ]; then
        kafka-topics.sh --delete --topic $2 --zookeeper 192.168.198.132:2181
    else
        echo $alert_str
    fi
;;


topic_list)
    kafka-topics.sh --list --zookeeper 192.168.198.132:2181
;;


*)
    echo $alert_str
;;
esac