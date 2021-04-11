import argparse
import json
import time
from random import choice
import datetime

from kafka import KafkaProducer


def _kafka_args():
    parser = argparse.ArgumentParser(add_help=False)
    args = parser.add_argument_group("kafka configurations")
    args.add_argument("-t", "--topic", help="Kafka topic.", default="kafka-default")
    args.add_argument(
        "-a", "--address", help="Kafka address.", default="localhost:9092"
    )
    args.add_argument("-u", "--username", help="Kafka server's username.")
    args.add_argument("-p", "--password", help="Kafka server's password.")
    return parser


def main():
    def default(o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

    parser = argparse.ArgumentParser(
        parents=[_kafka_args()], formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    args = parser.parse_args()

    if args.username and args.password:
        client = KafkaProducer(
            bootstrap_servers=args.address,
            sasl_plain_username=args.username,
            sasl_plain_password=args.password,
            sasl_mechanism="PLAIN",
            security_protocol="SASL_PLAINTEXT",
            acks=1,
            api_version=(1, 0),
        )
    else:
        client = KafkaProducer(bootstrap_servers=args.address)

    schools = ["harvard", "stanford", "oxford", "cambridge", "mit", "carnegie"]
    majors = [
        "computer science",
        "biology",
        "physics",
        "mechanical science",
        "materials science",
    ]
    names = ["lilei", "hanmeimei", "tom", "jerry", "leesin"]

    while True:
        data = {
            "school": choice(schools),
            "major": choice(majors),
            "name": choice(names),
            "timestamp": datetime.datetime.now(),
            "extra": "may be some other data",
        }
        msg = json.dumps(data, default=default)
        print("send kafka msg:", msg)
        client.send(args.topic, value=msg.encode("utf-8"))

        time.sleep(0.2)


if __name__ == "__main__":
    main()
