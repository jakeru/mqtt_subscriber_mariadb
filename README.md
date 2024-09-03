# MQTT subscriber to MariaDB

A Python application that subscribes to [MQTT](https://mqtt.org/) messages with
a specified topic and writes them to a [Maria Database](https://mariadb.org/).

## Installation

1. Clone this repository:
    ``` sh
    git clone https://github.com/jakeru/mqtt_subscriber_mariadb.git
    ```

2. Install a couple of needed system packages:
   ```sh
   apt install libmariadb-dev gcc python3-dev
   ```

3. Create and activate a Python virtual environment:
    ``` sh
    cd mqtt_db_writer
    python3 -m venv env
    source env/bin/activate

4. Install the pip packages listed in `requirements.txt`:

    ``` sh
    pip3 install -r requirements.txt
    ```

## Database preparation

This application connects to a Maria DB server and database of your choice. The
chosen database should have a table named `messages` with the structure defined
in
`db_init/mqtt.sql`.

The sql script makes the assumption that the database is named `mqtt`, but the
database can have any name.

## Usage

1. Activate the Python virtual environment.
    ``` sh
    source env/bin/activate
    ```

2. Run the application `mqtt_subscriber_mariadb.py` and supply the argument
   `--help` to get a list of possible parameters.
    ``` sh
    ./mqtt_subscriber_mariadb.py --help
    ```

## Test with `docker-compose`

A `docker-compose.yml` file that can be used to launch a
[Maria DB server](https://mariadb.org/) and the [Adminer](https://www.adminer.org/)
Database management tool is part of this repository.

To bring up the services, run:

``` sh
docker compose up -d
```

The `Adminer` tool can be accessed by visiting
[localhost:8080](http://localhost:8080) using your web browser. See the
`docker-compose.yml` file for the password for the user `root`.

Then launch this application:

``` sh
./mqtt_subscriber_mariadb.py --verbose
```

In another window, use the `mosquitto_pub` tool from the
[Eclipse Mosquitto MQTT broker](https://mosquitto.org/) like this:

``` sh
mosquitto_pub -d -t example_topic -m "This is a test"
```

The `mqtt_subscriber_mariadb.py` tool should report that it has written a
message to the database. You should also be able to see the written message
in the database using the `Adminer` tool.
