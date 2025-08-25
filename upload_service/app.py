from fastapi import FastAPI

# Cria a instância da aplicação FastAPI
app = FastAPI()

# Cria um endpoint de teste na raiz ("/")
@app.get("/")
def read_root():
    return {"message": "Serviço de Upload está funcionando!"}

# Aqui vamos adicionar o endpoint /upload no futuro
