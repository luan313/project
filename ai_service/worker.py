import pika
import json
import time
import os
import google.generativeai as genai
import sys

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'doc_processing_queue')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

if not GEMINI_API_KEY:
    print('[!] Erro: A variável de ambiente GEMINI_API_KEY não está definida.')
    sys.exit(1)

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

def process_message(channel, method, properties, body):
   
    print(" [x] Mensagem recebida")
    try:
        data = json.loads(body.decode('utf-8'))
        user_query = data.get('user_query')
        document_data = data.get('document_data')
        document_content = document_data.get('content')

        print(f"     - Pergunta do Utilizador: {user_query}")
        print(f"     - Dados do Documento: {document_data.get('filename')}")

        prompt = f"""
            Com base no conteúdo do documento recebido, responda a pergunta do utilizador.
            Conteúdo do Documento:
            "{document_content}"

            Pergunta do Utilizador:
            "{user_query}"
            """
        response = model.generate_content(prompt)
        ai_response = response.text
        print(f""" - Resposta da IA: {ai_response}""")

        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(" [x] Mensagem processada e confirmada (ack).")

    except Exception as e:
        print(f" [!] Erro ao processar a mensagem: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    connection = None
    print("A iniciar o worker do AI service...")
    while not connection:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print("Conectado ao RabbitMQ com sucesso!")
        except pika.exceptions.AMQPConnectionError:
            print("Falha ao conectar ao RabbitMQ. A tentar novamente em 5 segundos...")
            time.sleep(5)

    channel = connection.channel()
    print("Canal aberto.")

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"Fila '{QUEUE_NAME}' declarada.")

    channel.basic_qos(prefetch_count=1)
    print("Qualidade de Serviço (QoS) definida para 1.")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)
    print("Consumidor configurado.")

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
