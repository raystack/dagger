#!/bin/bash
timestamp_now=$(date +%s)
random_3char_suffix=$(openssl rand -base64 3)
random_enum_index=$(($RANDOM %3))
declare -a myArray=("FLIGHT" "BUS" "TRAIN")
cat sample_message.txt | \
sed "s/replace_timestamp_here/$timestamp_now/g; s/replace_service_type_here/${myArray[$random_enum_index]}/g; s/replace_customer_suffix_here/$random_3char_suffix/g" | \
protoc --proto_path=./ --encode=io.odpf.dagger.consumer.TestBookingLogMessage ./TestLogMessage.proto > message.bin
