#!/bin/sh

# This ..
#
FILE_JAR=exporter-1.0.1.jar
FILE_JAR=target/exporter-1.0.1.jar

FILE_SQL=file.sql
FILE_SQL=testData/fileIn01-db2.sql

DEF_JDBC="jdbc:mysql://localhost:3306/lenkodb"
DEF_JDBC="jdbc:db2://docker-dev.smartis.si:50000/testDb"
#DEF_JDBC="jdbc:db2://docker-dev.smartis.si:50000/testDb:allowNextOnExhaustedResultSet=1;"

java -jar $FILE_JAR -h

java -jar $FILE_JAR -s $FILE_SQL -d $DEF_JDBC -u db2inst1 -p "Passw00rd!"

echo That_s it.

exit 0
