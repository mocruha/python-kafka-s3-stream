import datetime
import json
import os
import subprocess
from time import sleep

from confluent_kafka import Producer

# Configuracao: variaveis de ambiente com default de desenvolvimento, para o
# quickstart funcionar sem .env (veja .env.example).
BROKER = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9094')
TOPIC = os.getenv('KAFKA_TOPIC', 'cpu_data')
SLEEP_INTERVAL = float(os.getenv('SAMPLE_INTERVAL', '1'))
CMD = ['mpstat', '-o', 'JSON']  # comando de coleta


def get_data(cmd: list) -> str:
    """
    Run a command and return the output.

    Args:
        cmd (list): The command to execute as a list of arguments.

    Returns:
        str: The stdout of the command as a string.
    """
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
        return result.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e}")
        return ""
    except Exception as e:
        print(f"Unexpected error: {e}")
        return ""


def delivery_report(err, msg):
    """
    Callback function to handle delivery reports for Kafka messages.

    Args:
        err: Error information (if any).
        msg: The Kafka message object.
    """
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} key=[cpu_data_{msg.key().decode()}]")


def produce_messages():
    """
    Continuously collect CPU data and produce messages to Kafka.
    """
    producer_config = {
        'bootstrap.servers': BROKER,
        'security.protocol': 'PLAINTEXT'
    }

    producer = Producer(producer_config)

    while True:
        data = get_data(CMD)
        if not data:
            print("No data received. Skipping this iteration.")
            sleep(SLEEP_INTERVAL)
            continue

        # mpstat e entrada externa: valida na borda e pula a iteracao se o
        # conteudo nao for JSON, em vez de publicar lixo no topico.
        try:
            json.loads(data)
        except json.JSONDecodeError as e:
            print(f"Malformed JSON from {CMD[0]}: {e}. Skipping this iteration.")
            sleep(SLEEP_INTERVAL)
            continue

        key = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S').encode('utf-8')
        try:
            # O stdout do mpstat ja e JSON. json.dumps() sobre essa string gravava
            # uma string escapada dentro do objeto: o bucket recebia
            # "{\"sysstat\"...}" em vez do JSON de verdade.
            value = data.encode('utf-8')
            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            producer.flush()
        except Exception as e:
            print(f"Error producing message: {e}")

        sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    produce_messages()
