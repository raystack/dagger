#!/bin/sh

docker compose up
sudo apt-get update
sudo apt-get install protoc
protoc --proto_path=./ --encode=io.odpf.dagger.consumer.TestPrimitiveMessage ./TestLogMessage.proto < ./sample_message.txt > out.bin
