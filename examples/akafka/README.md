# Basic Examples of Message-Publishing and -Subscribing Services based on Kafka

This directory contains two scripts that demostrate very basic
services which use the kafka functionality of pyquail.

## Usage:
Please note, to run these examples, a Kafka Broker must be running
at host "kafka" and port "9092" (as configured in the devcontainer).

First start the subscribing service:
```bash
python3 ./kafka_subscriber.py
```

Thereafter, in another terminal, execute the publishing
service:
```bash
python3 ./kafka_publisher.py
```

You should now see the following in the stdout of the subscribing
service:
```
{'count': 0}
{'count': 1}
{'count': 2}
{'count': 3}
{'count': 4}
{'count': 5}
{'count': 6}
{'count': 7}
{'count': 8}
{'count': 9}
```
