# alef-data-platform

Alef's Big data platform is maintained in this repository which is powered by -

* [Apache Kafka](http://kafka.apache.org/)
* [Apache Spark](https://spark.apache.org/)

This repository contains code to pull data sources from kafka and do transformation by both streaming and batch operations.

Module Overview
---------------

 * [Core](https://github.com/AlefEducation/alef-data-platform/tree/master/core) - This module is a framework for managing the Spark session for streaming and batch job.
 
 * ***-raw-events - Modules ending with raw events represents Spark Streaming operations.
 
 * ***-aggregation - Modules ending with this represents Spark batch operations.

Prerequisites for setting up project in Local
---------------------------------------------
This project uses Nexus proxy to resolve dependencies. The following 2 environment variables need to be exported to authenticate with Nexus Proxy Server.
```shell
export NEXUS_TOKEN_USR='<Nexus Token Username>'
export NEXUS_TOKEN_PSW='<Nexus Token Password>'
```
The credentials could be found in the AWS Secret Manager (Frankfrut) Region under `falcons/nexus`

Generate personal token for SonarCloud and run the following in terminal
```shell
export SONAR_TOKEN='<Sonar Cloud Token>'
```

Running the Project
-------------------

Both streaming and batch application can be run in local (development) environment without having access to the cluster.
 
 * Click on **Edit configurations...** under project run configuration.
 * Click on the **+** icon on the top left to **Add New Configuration**
 * Click on **Application** in the left pane and provide a **Name** in the right page.
 * In the configuration tab, enter the **Main Class** with extends the **SparkStreamingService** / **SparkBatchService** (full class path)
 * Add two more details
        
      * **VM options** - "-Dconfig.resource=live.conf" for batch pass additional props for ex: startDate.
      * **Environment Variables** - mode=debug;fs.s3a.access.key=xxxx;fs.s3a.secret.key=xxxx
        
 Now you can either run or debug the project.
 
 
 Git Working Style
 -----------------
 
 This repository follows open source style model of development and pull request. Hence its suggested to fork the repository rather than creating batches in parent repo.
 
 Steps:
   * Fork the repo from github.
   * Checkout the repo from your fork to development machine. 
   * Set **origin** to your fork and **upstream** to parent repo.

git remote -vv (sample)
-----------------------    
    origin  git@github.com:SarathChandran/alef-data-platform.git (fetch)
    origin  git@github.com:SarathChandran/alef-data-platform.git (push)
    upstream        git@github.com:AlefEducation/alef-data-platform.git (fetch)
    upstream        git@github.com:AlefEducation/alef-data-platform.git (push)
    
    
Cluster Details
---------------

    Spark Nodes:	
                master: ssh hadoop@18.184.83.232
                slave1: ssh hadoop@18.195.104.219
                slave2: ssh hadoop@18.184.184.159
    
    Presto / Hive	: ssh hadoop@18.194.235.140
    
    Kafka:	ssh hadoop@18.196.203.59
            ssh hadoop@18.196.233.134
    
    Kafka Connect:	
            ssh hadoop@35.158.207.82
            ssh hadoop@18.197.194.69

    Zookeeper:	
            hadoop@18.196.151.99
    
    S3 bucket:	
        s3://alef-db-bigdata-test  (contact fahim for access keys)
        Install "S3cmd" on Mac terminal and "Cyberduck" on Mac for GUI based software.

       
Logging
-------
Logging backend used in this project is `logback-classic` bridged with `log4j` used by Spark via `log4j-over-slf4j`.
In order to provide alternative logging configuration run JVM with `-Dlogback.configurationFile=/your/path/to/logback.xml`.

Running Test
------------
Make sure you configure aws locally before running the tests. Note: Add `eu-central-1` as default aws region while configuring aws.
```bash
make test
```
