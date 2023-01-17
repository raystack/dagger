#!/bin/bash
timestamp_now=$(date +%s)
random_enum_index=$(($RANDOM %3))
random_customer_id=$(($RANDOM %12))
declare -a myArray=("FLIGHT" "BUS" "TRAIN")
cat sample_message.txt | \
sed "s/replace_timestamp_here/$timestamp_now/g; s/replace_service_type_here/${myArray[$random_enum_index]}/g; s/replace_customer_suffix_here/$random_customer_id/g" | \
protoc --proto_path=io/odpf/dagger/consumer/ --encode=io.odpf.dagger.consumer.TestBookingLogMessage io/odpf/dagger/consumer/TestLogMessage.proto > message.bin
