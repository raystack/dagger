#!/bin/bash
timestamp_now=$(date +%s)
random_3char_suffix=$(pwgen 3 1)
random_enum_index=$(($RANDOM %3))
declare -a myArray=("FLIGHT" "BUS" "TRAIN")
cat sample_message.txt | \
sed "s/replace_timestamp_here/$timestamp_now/g; s/replace_service_type_here/${myArray[$random_enum_index]}/g; s/replace_customer_suffix_here/$random_3char_suffix/g" | \
protoc --proto_path=./ --encode=org.raystack.dagger.consumer.TestBookingLogMessage ./TestLogMessage.proto > message.bin
