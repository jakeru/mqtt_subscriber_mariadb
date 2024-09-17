#!/usr/bin/env python3

import argparse
import datetime
import logging
import sys
import json

# requires pip package pymongo
import pymongo

# requires pip package mariadb
import mariadb

# requires pip package paho-mqtt
import paho.mqtt.client as mqtt


class Backend:
    def write(self, msg):
        raise NotImplementedError()


class MariaDBBackend(Backend):
    def __init__(self, args):
        try:
            self.client = mariadb.connect(
                host=args.db_host,
                port=args.db_port,
                user=args.db_user,
                password=args.db_password,
                database=args.db_name,
                autocommit=True,
            )
            logging.info(
                f"Connected to MariaDB server '{args.db_host}' as '{args.db_user}'"
            )
        except mariadb.Error as e:
            logging.error(f"Failed to connect MariaDB server '{args.db_host}': {e}")
            raise

    def write(self, msg):
        cursor = self.client.cursor()
        try:
            cursor.execute(
                (
                    f"INSERT INTO messages "
                    "(time, topic, payload) "
                    "VALUES (NOW(), ?, ?)"
                ),
                (msg.topic, msg.payload),
            )
            logging.debug(
                "MariaDB: Message with topic '%s' of size %d B written with id %d",
                msg.topic,
                len(msg.payload),
                cursor.lastrowid
            )
            return True
        except mariadb.Error as e:
            logging.error(f"Failed to write message to database: {e}")


class MongoDBBackend(Backend):
    def __init__(self, args):
        url = (
            f"mongodb://{args.mongodb_user}:{args.mongodb_password}@"
            f"{args.mongodb_host}:{args.mongodb_port}/"
        )
        self.client = pymongo.MongoClient(url)
        self.db = self.client[args.mongodb_db]
        self.collection = self.db[args.mongodb_collection]
        logging.info(
            "Connected to '%s' using database '%s' collection '%s'",
            args.mongodb_host,
            args.mongodb_db,
            args.mongodb_collection,
        )

    def write(self, msg):
        try:
            message = json.loads(msg.payload)
        except json.decoder.JSONDecodeError as e:
            logging.warning("Failed to parse string '%s' as json: %s", msg.payload, e)
            return True
        document = {
            "time": datetime.datetime.now(tz=datetime.timezone.utc),
            "topic": msg.topic,
            "message": message,
        }
        res = self.collection.insert_one(document)
        logging.debug(
            "MongoDB: Message with topic '%s' written with id '%s'",
            msg.topic,
            res.inserted_id,
        )
        return True


def on_connect(client, userdata, _flags, _rc, _properties):
    topics = userdata["args"].mqtt_topic
    logging.info(
        "Connected to MQTT server %s port %d using %s",
        client.host,
        client.port,
        client.transport,
    )
    for topic in topics:
        logging.info("Subscribing to topic '%s'", topic)
        client.subscribe(topic, 0)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    if userdata["exit_code"] != 0:
        # We are about to exit due to an error. No need to log anything.
        return
    logging.warning("Disconnected from MQTT server. Will try to reconnect soon.")


def on_connect_fail(client, userdata):
    logging.debug("Failed to connect to MQTT server. Will retry soon again.")


def on_message(client, userdata, msg):
    for backend in userdata["backends"]:
        if not backend.write(msg):
            userdata["exit_code"] = -2
            client.disconnect()
            break


def parse_args():
    parser = argparse.ArgumentParser(
        description="MQTT database writer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", help="Verbose mode", action="store_true")
    parser.add_argument(
        "--mqtt_host",
        default="localhost",
        help="The MQTT broker address",
    )
    parser.add_argument(
        "--mqtt_port",
        type=int,
        default=1883,
        help="The MQTT broker port",
    )
    parser.add_argument(
        "--mqtt_keepalive",
        type=int,
        default=30,
        help="The MQTT keepalive interval (in seconds)",
    )
    parser.add_argument(
        "-t",
        "--mqtt_topic",
        required=True,
        metavar="TOPIC",
        action="append",
        help="The topic(s) to subscribe to ('#' for all topics). Can be specified multiple times",
    )
    parser.add_argument(
        "--mariadb_host",
        help="The address to the MariaDB server",
    )
    parser.add_argument(
        "--mariadb_port",
        default=3306,
        type=int,
        help="The port of the MariaDB server",
    )
    parser.add_argument(
        "--mariadb_user",
        help="The database user",
    )
    parser.add_argument(
        "--mariadb_password",
        help="The database password",
    )
    parser.add_argument(
        "--mariadb_name",
        default="mqtt",
        help="The name of the database",
    )
    parser.add_argument(
        "--mongodb_host",
        help="MongoDB server address",
    )
    parser.add_argument(
        "--mongodb_port",
        default=27017,
        type=int,
        help="MongoDB server port",
    )
    parser.add_argument(
        "--mongodb_db",
        default="mqtt",
        help="MongoDB database name",
    )
    parser.add_argument(
        "--mongodb_collection",
        default="messages",
        help="MongoDB collection",
    )
    parser.add_argument(
        "--mongodb_user",
        default="root",
        help="MongoDB user",
    )
    parser.add_argument(
        "--mongodb_password",
        default="",
        help="MongoDB password",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = "%(asctime)-15s %(levelname)-7s %(name)-6s %(message)s"
    logging.basicConfig(format=log_format, level=log_level)
    for level in (logging.DEBUG, logging.INFO):
        logging.addLevelName(level, logging.getLevelName(level).lower())

    backends = []
    if args.mariadb_host:
        backends.append(MariaDBBackend(args))
    if args.mongodb_host:
        backends.append(MongoDBBackend(args))

    if not backends:
        logging.warning(
            (
                "No backend specified. Hint: use '--mariadb_host' and/or "
                "'--mongodb_host' to specify"
            )
        )

    userdata = {"backends": backends, "args": args, "exit_code": 0}
    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        userdata=userdata,
        reconnect_on_failure=True,
    )

    # Assign event callbacks
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_connect_fail = on_connect_fail

    # Connect
    mqttc.connect(args.mqtt_host, args.mqtt_port, args.mqtt_keepalive)

    logging.info("Running until stopped with ctrl+c...")

    # Continue the network loop & close db-connection
    mqttc.loop_forever()
    sys.exit(userdata["exit_code"])


if __name__ == "__main__":
    main()
