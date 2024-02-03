cd scripts


### raw_data_processing ###

python data_processing.py s3a://yelp-data-segmentation/input  s3a://yelp-data-segmentation/output/ 0.1 > processing.log 2>&1

### Kafka Producer #####

python3 kafka/producer.py 54.193.66.237:9092 reviews /home/ubuntu/yelp_academic_dataset_review_test.json 500 > ~/producer.log 2>&1

### Kafka Ingestion ####

python3 data_ingestion.py 54.193.66.237:9092 reviews s3a://yelp-data-segmentation/output/ 0.1 > ~/ingestion.log 2>&1

### raw_data_processing ###

python3 feature_aggregator.py 0.1 > features.log 2>&1