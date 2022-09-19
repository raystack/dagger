# Dagger Quickstart

## Before beginning, please ensure the following:

1. **Your Java version is Java 8**: Dagger as of now works only with Java 8. Some features might not work with older or later versions.
2. Your **Kafka** version is **3.0.0** or a minor version of it
3. You have **kcat** installed: We will use kcat to push messages to Kafka from the CLI. You can follow the installation steps [here](https://github.com/edenhill/kcat). Ensure the version you install is 1.7.0 or a minor version of it.
4. You have **protobuf** installed: We will use protobuf to push messages encoded in protobuf format to Kafka topic. You can follow the installation steps for MacOS [here](https://formulae.brew.sh/formula/protobuf). For other OS, please download the corresponding release from [here](https://github.com/protocolbuffers/protobuf/releases). Please note, this quickstart has been written to work with[ 3.17.3](https://github.com/protocolbuffers/protobuf/releases/tag/v3.17.3) of protobuf. Compatibility with other versions in unknown.
5. You have **Python 2.7+** and **simple-http-server** installed: We will use Python along with simple-http-server to spin up a mock Stencil server which can serve the proto descriptors to Dagger. To install **simple-http-server**, please follow these [installation steps](https://pypi.org/project/simple-http-server/).

## Quickstart

1. 
