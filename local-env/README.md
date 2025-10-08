# Local Env
It enables developer to run the project in **local machine** without relying on any external system. At present developer needs to set up their own data. All setup data is retained in the `<PROJECT ROOT>/local-env/.data/` dir of the developer machine. 

#### Prerequisites
    - docker
    - shell

### How to Use

1. Spin up the dockerized local environment by running the following command from the project root dir.
    ```shell
    make local
    ```
Once the above command is successful, the following applications can be connected from the docker host machine: 
- **Kafka:** `localhost:9093`

2. Setup data in Kafka.
   - Create required topic
   - Publish data to topic using either from command line or from [Conduktor](www.conduktor.io)
    ```shell
    # Using Kafka Console Producer / Consumer
    kafka-console-producer --broker-list localhost:9093 --topic test
    kafka-console-consumer --bootstrap-server localhost:9093 --topic test --from-beginning

    # Using kafkacat
    kafkacat -b localhost:9093 -t test -P
    kafkacat -b localhost:9093 -t test -C

    # Produce with Header
    kafkacat -b localhost:9092 -t test -P -H foo=bar
    ```
3. While running the **Streaming Job**, update the following env variables
    ```shell
    KAFKA_BROKERS=localhost:9093
    PROTOCOL=PLAINTEXT
    S3_ENDPOINT=localhost:4566;S3_PATH_STYLE_ACCESS=true;S3_SSL_ENABLED=false
    ```
4. While running the **Batch Job**, update the following env variables
    ```S3_ENDPOINT=localhost:4566;S3_PATH_STYLE_ACCESS=true;S3_SSL_ENABLED=false;EXECUTION_ENV=local```
5. Create alef-bigdata-emr bucket locally for the first time
   ```aws --endpoint-url=http://localhost:4566 s3 mb  s3://alef-bigdata-emr```
   
6. Read the file from the following location
 ```http://localhost:4566/alef-bigdata-emr/alef-data-platform/{env}/incoming/```

7. Using localstack we can not flow data into redshift, because redshift is not supported in localstack, hence I have skipped processing of redshift sinks when local config is provided in batch jobs. So now if you run the batch job using local config it will only flow data into delta and not redshift.