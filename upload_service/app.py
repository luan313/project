import pika
import json
from fastapi import FastAPI, UploadFile, File, Form
from typing import Annotated

# Cria a instância da aplicação FastAPI
app = FastAPI()

# --- Configuração do RabbitMQ ---
# O ideal é ler isso de variáveis de ambiente
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'doc_processing_queue'

# Cria um endpoint de teste na raiz ("/")
@app.get("/")
def read_root():
    return {"message": "Serviço de Upload está funcionando!"}

# Endpoint para upload de documentos
@app.post("/upload")
async def upload_document(
    file: Annotated[UploadFile, File()],
    user_query: Annotated[str, Form()]
):
    """
    Recebe um arquivo e uma pergunta do usuário,
    e publica uma mensagem no RabbitMQ para processamento.
    """
    print(f"Recebido arquivo: {file.filename}, Pergunta: '{user_query}'")

    # Por enquanto, vamos apenas ler o nome do arquivo como exemplo.
    # No futuro, aqui entrará a lógica para ler o conteúdo (docx, pdf, etc.)
    file_content = {"filename": file.filename, "content": "Conteúdo do arquivo iria aqui..."}

    # Monta a mensagem que será enviada para a fila
    message = {
        'user_query': user_query,
        'document_data': file_content
    }

    try:
        # Conecta ao RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()

        # Garante que a fila existe
        channel.queue_declare(queue=QUEUE_NAME)

        # Publica a mensagem na fila
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message)
        )

        print("Mensagem publicada no RabbitMQ com sucesso!")
        connection.close()

        return {"status": "success", "message": "Documento enviado para a fila de processamento."}

    except Exception as e:
        print(f"Erro ao conectar ou publicar no RabbitMQ: {e}")
        return {"status": "error", "message": "Não foi possível enviar o documento para processamento."}

