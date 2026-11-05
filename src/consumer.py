import io
import os

from confluent_kafka import Consumer, KafkaError
from minio import Minio

# Configuracao: variaveis de ambiente com default de desenvolvimento, para o
# quickstart funcionar sem .env (veja .env.example).
BROKER = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9094')
TOPIC = os.getenv('KAFKA_TOPIC', 'cpu_data')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'cpu_group')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'baldinho')


def consume_messages(minio_cli: Minio) -> None:
    """Consome mensagens do Kafka e grava cada uma como objeto no MinIO."""
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaError(message.error())

            key = (message.key() or b'').decode('utf-8')
            print(f"Received message: key={key}")

            minio_put_message(minio_cli, message.value(), key)

    except KeyboardInterrupt:
        print("\nConsumer interrupted by user.")
    finally:
        consumer.close()
        print("Consumer closed gracefully.")


def create_minio_client() -> Minio:
    """Cria o client do MinIO a partir da configuracao."""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def minio_put_message(minio_cli: Minio, message: bytes, name: str) -> None:
    """Grava a mensagem como um objeto JSON no bucket configurado.

    O nome do objeto e derivado da key da mensagem, entao uma reentrega
    sobrescreve o mesmo objeto em vez de duplicar (idempotencia).

    Parameters:
        minio_cli: Client do MinIO ja configurado.
        message: Valor da mensagem (bytes).
        name: Key da mensagem, usada para compor o nome do objeto.
    """
    minio_cli.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=f'cpu_{name}.json',
        data=io.BytesIO(message),
        length=len(message),
        content_type='application/json'
    )


if __name__ == "__main__":
    consume_messages(create_minio_client())
