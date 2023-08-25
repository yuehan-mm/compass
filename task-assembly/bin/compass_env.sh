#!/bin/bash

# dolphinscheduler or airflow or custom
export SCHEDULER="airflow"
export SPRING_PROFILES_ACTIVE="hadoop,${SCHEDULER}"

# Scheduler MySQL
export SCHEDULER_MYSQL_ADDRESS="10.138.46.222:3306"
export SCHEDULER_MYSQL_DB="airflow"
export SCHEDULER_DATASOURCE_URL="jdbc:mysql://${SCHEDULER_MYSQL_ADDRESS}/${SCHEDULER_MYSQL_DB}?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
export SCHEDULER_DATASOURCE_USERNAME="scan_binlog"
export SCHEDULER_DATASOURCE_PASSWORD="Scan_Binlog123!@#"

# Compass MySQL
export COMPASS_MYSQL_ADDRESS="10.138.46.222:3306"
export COMPASS_MYSQL_DB="compass"
export SPRING_DATASOURCE_URL="jdbc:mysql://${COMPASS_MYSQL_ADDRESS}/${COMPASS_MYSQL_DB}?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
export SPRING_DATASOURCE_USERNAME="bdp_jt"
export SPRING_DATASOURCE_PASSWORD="OdKzCYC4s"

export SPRING_HDOPDB_MYSQL_ADDRESS="10.138.46.222:3306"
export SPRING_HDOPDB_MYSQL_DB="bdmp_cluster"
export SPRING_HDOPDB_URL="jdbc:mysql://${SPRING_HDOPDB_MYSQL_ADDRESS}/${SPRING_HDOPDB_MYSQL_DB}?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
export SPRING_HDOPDB_USERNAME="cluster_manager"
export SPRING_HDOPDB_PASSWORD="cluster_MANAGER@!#$13"

# Kafka
export SPRING_KAFKA_BOOTSTRAPSERVERS="10.163.137.150:9092,10.163.137.151:9092,10.163.137.152:9092"
# Optional
export SPRING_KAFKA_CONSUMER_SECURITYPROTOCOL="SASL_PLAINTEXT"
export SPRING_KAFKA_CONSUMER_SASLMECHANISM="SCRAM-SHA-512"
export SPRING_KAFKA_CONSUMER_SASLJAASCONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username='hdop' password='v41ieoEFhnPajg5L';"

# Redis
export SPRING_REDIS_HOST="10.163.199.178"
export SPRING_REDIS_PORT="6379"
# Optional
export SPRING_REDIS_PASSWORD="bigdata@690"

# Elasticsearch
export SPRING_ELASTICSEARCH_NODES="10.138.225.200:27521"
# Optional
export SPRING_ELASTICSEARCH_USERNAME="elastic"
# Optional
export SPRING_ELASTICSEARCH_PASSWORD="elastichaier"

# task-syncer
# source mysql
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_URL=${SCHEDULER_DATASOURCE_URL}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_USERNAME=${SCHEDULER_DATASOURCE_USERNAME}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_SOURCE_PASSWORD=${SCHEDULER_DATASOURCE_PASSWORD}
# destination mysql
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_URL=${SPRING_DATASOURCE_URL}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_USERNAME=${SPRING_DATASOURCE_USERNAME}
export SPRING_DATASOURCE_DYNAMIC_DATASOURCE_DIAGNOSE_PASSWORD=${SPRING_DATASOURCE_PASSWORD}

# compass db topic
export SPRING_KAFKA_MYSQLDATATOPICS="test_airflow_cdc_data"
# compass db consumer
export SPRING_KAFKA_CONSUMER_SYNCERGROUPID="test_airflow_cdc_data_22047328_d"

# task_instance topic
export SPRING_KAFKA_TASKINSTANCETOPICS="compass_test"
export DATASOURCE_WRITEKAFKATOPIC_TASKINSTANCE=${SPRING_KAFKA_TASKINSTANCETOPICS}

# task_instance_application topic
export SPRING_KAFKA_TASKINSTANCEAPPLICATIONTOPICS="compass_taskinstance_application"
# task_instance consumer
export SPRING_KAFKA_CONSUMER_TASKAPPGROUPID="compass_test_22047328_d"
#export SPRING_KAFKA_CONSUMER_DETECTGROUPID="compass_test_22047328"
export SPRING_KAFKA_CONSUMER_DETECTGROUPID="compass_taskinstance_application_20012523_test"

# task_record
export SPRING_KAFKA_TASKRECORD_TOPIC="compass_taskrecord_test"
export SPRING_KAFKA_TASKRECORD_GROUPID="compass_taskrecord_test_20012523_consumer"

# hdfs base path
export SPRING_HDFS_BASEPATH="hdfs://nameservice1/flume/airflow_test/bigdata/bdp_jt/airflow/logs"