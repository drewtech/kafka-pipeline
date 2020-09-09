## Kafka processing with python and S3 - A example datapipeline

Add json to messages.txt.
The producer will pick them up, then transformer with change the data, add to another pipeline, onto a 3rd consumer.

You can view the kafka pipeline with ([Kafdrop](https://github.com/obsidiandynamics/kafdrop)), it is defined in docker-compose.yml

```bash
$ localhost:9000
```

## Pre-requisites

Docker, docker-compose.

To run the producer/consumers locally, install python3,  then use pip3 to install the pre-requsites in requirements.txt 

## Local Development

Build:

```bash
$ docker-compose build
```

Run:

```bash
$ ./scripts/run.sh
```

## Test Suite
Run tests:

TBA

#### Debugging

TBA

#### Logs

Logs are output to stdout and file logs.



