# Datahub Handler for Oracle GoldenGate

[中文版](https://github.com/aliyun/aliyun-odps-ogg-plugin/wiki)

The datahub handler processes the operations of OGG trails, and upload the change logs into ODPS datahub. It has the following features:

- Support 3 types of operation logs: *INSERT*, *UPDATE*, *DELETE*
- User customize key fields, focus fields, and corresponding ODPS field types
- Support ODPS partition table, create partition automatically if not exists
- Provide table tool of ODPS, which can create and delete ODPS tables according to the configuration file of the handler
- Provide alarm interface to support user implement alarm module

## Getting Started
---

### Requirements

1. JDK 1.6 or later (JDK 1.7 recommended)
2. Apache Maven 3.x
3. OGG Java Adapter

### Clone and Build the Package

Clone the project from github:

```
$ git clone git@github.com:aliyun/aliyun-odps-ogg-plugin.git
```

Build the package with the script file:

```
$ cd aliyun-odps-ogg-plugin/
$ ./build.sh
```

Wait until building success. The datahub handler will be in **aliyun-odps-ogg-plugin/build_1.0.0/datahub_lib/**

### Use Datahub Handler

1. Configure datahub handler for OGG extract.

    Set goldengate writers to `jvm`:

    ```
    goldengate.userexit.writers=jvm
    ```

    Set jvm boot options and goldengate classpath:

    ```
    jvm.bootoptions=-Djava.class.path=ggjava/ggjava.jar -Xmx512m -Xms32m 
    gg.classpath=/.../datahub_lib/*
    ```

    Set the handler name and type:

    ```
    gg.handlerlist=ggdatahub
    gg.handler.ggdatahub.type=com.aliyun.odps.ogg.handler.datahub.DatahubHandler
    ```

2. Configure other handler parameters.

3. Start OGG extract, then the handler will start uploading data into ODPS datahub.

### Handler Parameter Details

Following is a sample properties file of OGG datahub handler:


```
# extdatahub.properties

# Set handler name
gg.handlerlist=ggdatahub

# Set goldengate classpath
gg.classpath=YOUR_DATAHUB_HANDLER_DIRECTORY/datahub_lib/*

# Set jvm boot options
jvm.bootoptions=-Djava.class.path=ggjava/ggjava.jar -Xmx512m -Xms32m

# Handler type of ggdatahub, need to be exactly the same as below 
gg.handler.ggdatahub.type=com.aliyun.odps.ogg.handler.datahub.DatahubHandler

# Custom implemention of alarm interface (make sure that the related jars were put in the classpath). Default alarm implementation is LogAlarm that records the logs using log4j.
gg.handler.ggdatahub.alarmImplement=com.test.LogAlarm

# ODPS project name
gg.handler.ggdatahub.project=YOUR_PROJECT_NAME

# ODPS table name. The table should be created before starting OGG extract. The table tool can be used to create target table according to this properties file.
gg.handler.ggdatahub.tableName=gg_test

# Aliyun access_id
gg.handler.ggdatahub.accessID=YOUR_ACCESS_ID

# Aliyun access_key
gg.handler.ggdatahub.accessKey=YOUR_ACCESS_KEY

# ODPS end point. For applications running on Aliyun ECS, use http://odps-ext.aiyun-inc.com/api, otherwise, use http://service.odps.aliyun.com/api
gg.handler.ggdatahub.endPoint=http://service.odps.aliyun.com/api

# ODPS datahub end point. For applications running on Aliyun ECS, use http://dh-ext.odps.aliyun-inc.com, otherwise, use http://dh.odps.aliyun.com
gg.handler.ggdatahub.datahubEndPoint=http://dh.odps.aliyun.com

# Shard number of the ODPS table
gg.handler.ggdatahub.shardNumber=1

# Hublifecycle of the ODPS table
gg.handler.ggdatahub.hubLifeCycle=7

# Load shard timeout in seconds
gg.handler.ggdatahub.shardTimeout=60

# The maximum capacity of the handler to hold the operations. When the number of the operations in a transaction reaches batchSize, the handler will upload the records to the ODPS immediately. This parameter is used for avoiding OutOfMemory.
gg.handler.ggdatahub.batchSize=1000

# Path to the file which records the handler information. Defaulting to dirdat/%handler_name%_handler_info
gg.handler.ggdatahub.handlerInfoFileName=dirdat/ggdatahub_handler_info

# Specify which field of ODPS table should store the operation type. If skip this, no operation type would be stored.
gg.handler.ggdatahub.operTypeField=operation_type

# Specify which field of ODPS table should store the operation time. If skip this, no operation time would be stored.
gg.handler.ggdatahub.operTimeField=operation_time

# Specify the field names of the keys in Oracle, and the corresponding data-type in ODPS. Datahub handler will parse each value of these fields to the specified data-type, and put the results in the fields with the same name in ODPS table
gg.handler.ggdatahub.keyFields=trans_id/STRING,item_id/STRING

# Specify the field names of the columns to be watched in Oracle, and the corresponding data-type in ODPS. Datahub handler will parse each value of the these fields to the specified data-type. The fields that append "_before", "_after" to the original field names will be used to store the values before change, after change, respectively.
gg.handler.ggdatahub.focusFields=total_sales/DOUBLE

# Oracle dateformat for converting the date fields from Oracle to ODPS. This can be omit if no date fields exist.
gg.handler.ggdatahub.dbDateFormat=yyyy-MM-dd:HH:mm:ss

# Names of ODPS partition fields, seperated by comma.
gg.handler.ggdatahub.partitionFields=pt,dt,item

# Values of ODPS partition fields. The sequence should be consistent with partition names.
# 3 ways are provided to customize the partition values
#    1. Using fixed value
#    2. Using escape sequences for the timestamp (Details about timestamp are introduced next)
#    3. Using values from specified field
gg.handler.ggdatahub.partitionValues=p1,%y-%m-%d,%{item_id}

# Handler uses the start_time of the transaction as timestamp by default. The date field values of ODPS table can be used instead.
gg.handler.ggdatahub.timestampField=YOUR_TIMESTAMP_FIELDNAME

# Dateformat of the timestamp, neccessary if the timestampField is changed. This will be used to parse the field values to the timestamp.
gg.handler.ggdatahub.timestampDateFormat=yyyy-MM-dd HH:mm:ss.SSSSSS

# Specify the maximum retry count of write pack. Defaulting to 3.
gg.handler.ggdatahub.retryCount=3

# Specify how many failed operations can be omit. Defaulting to 0.
gg.handler.ggdatahub.passFailedOperationCount=10

# Path to the file which records the failed operations. Defaulting to dirrpt/%handler_name%_bad_operations.log
gg.handler.ggdatahub.failedOperationFileName=dirrpt/ggdatahub_bad_operations.log

```

The following are the escape sequences supported:

Alias | Description
---|---
%t|Unix time in milliseconds
%a|locale’s short weekday name (Mon, Tue, ...)
%A|locale’s full weekday name (Monday, Tuesday, ...)
%b|locale’s short month name (Jan, Feb, ...)
%B|locale’s long month name (January, February, ...)
%c|locale’s date and time (Thu Mar 3 23:05:25 2005)
%d|day of month (01)
%D|date; same as %m/%d/%y
%H|hour (00..23)
%I|hour (01..12)
%j|day of year (001..366)
%k|hour ( 0..23)
%m|month (01..12)
%M|minute (00..59)
%p|locale’s equivalent of am or pm
%s|seconds since 1970-01-01 00:00:00 UTC
%S|second (00..59)
%y|last two digits of year (00..99)
%Y|year (2010)
%z|+hhmm numeric timezone (for example, -0400)


### Use Table Tool 

The table tool in the **datahub_handler/table_tool/** directory can create/drop ODPS table according to the properties file.

Example：

```
$ cd datahub_handler/table_tool/
$ java -jar ogg-odps-table-1.0.0.jar CREATE %YOUR_OGG_DIRECTORY%/dirprm/extdatahub.properties
$ java -jar ogg-odps-table-1.0.0.jar DROP %YOUR_OGG_DIRECTORY%/dirprm/extdatahub.properties
```

If multiple handlers were configured in the properties file, the table tool need to know which handler to process.

```
$ java -jar ogg-odps-table-1.0.0.jar CREATE %YOUR_OGG_DIRECTORY%/dirprm/extdatahub.properties ggdatahub
```

### Use Custom Alarm

The Alarm interface have 4 methods with 2 different alarm level, *warn* and *error*. The warn level indicates there might be some problem, but the handler can keep working.
The error level indicates a fatal error. In most cases, the OGG extract will abend when a fatal error occurred.

Following are the methods in the alarm interface:

```
public interface OggAlarm {
    public void warn(String msg);
    public void warn(String msg, Throwable throwable);

    public void error(String msg);
    public void error(String msg, Throwable throwable);
}
```

A default implementation is LogAlarm that records the logs using *log4j*.

## Authors && Contributors
---

- [Tian Li](https://github.com/tianliplus)
- [Yang Hongbo](https://github.com/hongbosoftware)

## License
---

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
