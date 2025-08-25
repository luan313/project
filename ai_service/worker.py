import pika
import json
import time
import os

# --- Configuração do RabbitMQ lida das variáveis de ambiente ---
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'doc_processing_queue')

def process_message(channel, method, properties, body):
    """
    Função callback que é executada sempre que uma mensagem é recebida da fila.
    """
    print(" [x] Mensagem recebida")
    try:
        # Converte o corpo da mensagem (que está em bytes) para um dicionário Python
        data = json.loads(body.decode('utf-8'))
        user_query = data.get('user_query')
        document_data = data.get('document_data')

        print(f"     - Pergunta do Utilizador: {user_query}")
        print(f"     - Dados do Documento: {document_data.get('filename')}")

        # --- A LÓGICA DE CHAMADA À IA ENTRARIA AQUI ---
        # Por agora, vamos apenas simular um tempo de processamento.
        print("     - [Simulação] A processar com a IA...")
        time.sleep(3) # Simula um trabalho de 3 segundos
        print("     - [Simulação] Processamento concluído.")

        # Confirma ao RabbitMQ que a mensagem foi processada com sucesso.
        # Isto remove a mensagem da fila.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(" [x] Mensagem processada e confirmada (ack).")

    except Exception as e:
        print(f" [!] Erro ao processar a mensagem: {e}")
        # Rejeita a mensagem sem a recolocar na fila para evitar loops de erro.
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    connection = None
    # Tenta conectar ao RabbitMQ repetidamente até conseguir.
    # Isto é útil porque o worker pode iniciar antes do RabbitMQ estar pronto.
    while not connection:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print("Conectado ao RabbitMQ com sucesso!")
        except pika.exceptions.AMQPConnectionError:
            print("Falha ao conectar ao RabbitMQ. A tentar novamente em 5 segundos...")
            time.sleep(5)

    channel = connection.channel()

    # Garante que a fila existe (deve ser a mesma do serviço de upload)
    channel.queue_declare(queue=QUEUE_NAME)

    # Define que este worker só deve pegar 1 mensagem de cada vez.
    channel.basic_qos(prefetch_count=1)

    # Associa a nossa função `process_message` à fila.
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

    print(' [*] A aguardar por mensagens. Para sair, pressione CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrompido')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
