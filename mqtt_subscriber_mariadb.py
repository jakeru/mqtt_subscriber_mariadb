#!/usr/bin/env python3

import argparse
import logging

# requires pip package mariadb
import mariadb

# requires pip package paho-mqtt
import paho.mqtt.client as mqtt


def on_connect(client, userdata, _flags, _rc, _properties):
    topic = userdata["args"].mqtt_topic
    logging.info("Connected to MQTT server, subscribing to '%s'", topic)
    client.subscribe(topic, 0)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    logging.warning("Disconnected from MQTT server. Will try to reconnect soon.")


def on_connect_fail(client, userdata):
    logging.debug("Failed to connect to MQTT server. Will retry soon again.")


def on_message(_client, userdata, msg):
    cursor = userdata["cursor"]
    try:
        cursor.execute(
            (f"INSERT INTO messages " "(time, topic, payload) " "VALUES (NOW(), ?, ?)"),
            (msg.topic, msg.payload),
        )
        logging.debug(
            "Message to topic '%s' of size %d B written with id %d",
            msg.topic,
            len(msg.payload),
            cursor.lastrowid,
        )
    except mariadb.Error as e:
        logging.error("Failed to write message to database: %s", e)


def parse_args():
    parser = argparse.ArgumentParser(
        description="MQTT database writer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--verbose", help="Verbose mode", action="store_true")
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
        "--mqtt_topic",
        default="#",
        help="The topic to subscribe to",
    )
    parser.add_argument(
        "--db_host",
        default="127.0.0.1",
        help="The address to the MariaDB server",
    )
    parser.add_argument(
        "--db_port",
        default=3306,
        type=int,
        help="The port of the MariaDB server",
    )
    parser.add_argument(
        "--db_name",
        default="mqtt",
        help="The name of the database",
    )
    parser.add_argument(
        "--db_user",
        help="The database user",
    )
    parser.add_argument(
        "--db_password",
        help="The database password",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = "%(asctime)-15s %(levelname)-7s %(name)-6s %(message)s"
    logging.basicConfig(format=log_format, level=log_level)

    dbc = mariadb.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        autocommit=True,
    )
    cursor = dbc.cursor()

    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        userdata={"cursor": cursor, "args": args},
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
    dbc.close()


if __name__ == "__main__":
    main()
