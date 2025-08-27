import pika
import json
import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from typing import Annotated
# from docling.document_converter import DocumentConverter 
import tempfile
import shutil

app = FastAPI()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'doc_processing_queue')

def read_document_content(file: UploadFile) -> str:
    """
    Lê o conteúdo de um ficheiro usando a biblioteca docling.
    Guarda o ficheiro enviado num local temporário para ser processado.
    """

    from docling.document_converter import DocumentConverter


    with tempfile.NamedTemporaryFile(delete=False, suffix=file.filename) as tmp_file:
        shutil.copyfileobj(file.file, tmp_file)
        tmp_file_path = tmp_file.name

    try:
        print(f"A processar com a docling: {tmp_file_path}")
        converter = DocumentConverter()
        doc = converter.convert(tmp_file_path).document
        return doc.export_to_markdown()
    finally:
        os.remove(tmp_file_path)

@app.get("/")
def read_root():
    return {"message": "Serviço de Upload está funcionando!"}

@app.post("/upload")
async def upload_document(
    file: Annotated[UploadFile, File()],
    user_query: Annotated[str, Form()]
):
    print(f"Recebido ficheiro: {file.filename}, Pergunta: '{user_query}'")

    allowed_extensions = ['.docx', '.pdf', '.pptx', '.txt']
    if not any(file.filename.endswith(ext) for ext in allowed_extensions):
        raise HTTPException(status_code=400, detail=f"Tipo de ficheiro não suportado. Por favor, envie um dos seguintes: {allowed_extensions}")

    document_text = read_document_content(file)
    
    file_content = {"filename": file.filename, "content": document_text}

    message = {'user_query': user_query, 'document_data': file_content}

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode = 2)
        )
        print("Mensagem publicada no RabbitMQ com sucesso!")
        connection.close()
        return {"status": "success", "message": "Documento enviado para a fila de processamento."}
    except Exception as e:
        print(f"Erro ao conectar ou publicar no RabbitMQ: {e}")
        raise HTTPException(status_code=500, detail="Não foi possível enviar o documento para processamento.")
