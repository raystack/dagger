#!/bin/bash
set -x
timestamp_now=$(date +%s)
random_3char_suffix=$(openssl rand -base64 3)
random_enum_index=$(($RANDOM %3))
declare -a myArray=("GO_RIDE" "GO_SEND" "GO_SHOP")

cat testbookinglogmessage.text | \
sed "s/replace_timestamp_here/$timestamp_now/g; s/replace_service_type_here/${myArray[$random_enum_index]}/g; s/replace_customer_suffix_here/$random_3char_suffix/g" | \
protoc  --proto_path=/Users/kevinbheda/workspace/dagger/dagger-common/src/test/proto \
 /Users/kevinbheda/workspace/dagger/dagger-common/src/test/proto/TestLogMessage.proto \
 --encode io.odpf.dagger.consumer.TestBookingLogMessage  > /tmp/message$random_3char_suffix.bin
 kafkacat -P -b localhost:9092 -t test-topic /tmp/message$random_3char_suffix.bin

