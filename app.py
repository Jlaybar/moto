from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import logging
from config import Config

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar FastAPI
app = FastAPI(
    title="ChatGPT API Backend",
    description="Backend para conectar con ChatGPT",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especifica los dominios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar cliente OpenAI
client = OpenAI(api_key=Config.OPENAI_API_KEY)

# Modelos de datos
class QuestionRequest(BaseModel):
    question: str
    model: str = "gpt-3.5-turbo"
    max_tokens: int = 500
    temperature: float = 0.7

class QuestionResponse(BaseModel):
    answer: str
    model: str
    tokens_used: int

# Endpoint de salud
@app.get("/")
async def root():
    return {"message": "ChatGPT API Backend funcionando correctamente"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "chatgpt-backend"}

# Endpoint principal para hacer preguntas
@app.post("/ask", response_model=QuestionResponse)
async def ask_question(request: QuestionRequest):
    try:
        logger.info(f"Pregunta recibida: {request.question}")
        
        # Llamar a la API de OpenAI
        response = client.chat.completions.create(
            model=request.model,
            messages=[
                {"role": "system", "content": "Eres un asistente útil y amigable."},
                {"role": "user", "content": request.question}
            ],
            max_tokens=request.max_tokens,
            temperature=request.temperature
        )
        
        # Extraer la respuesta
        answer = response.choices[0].message.content
        tokens_used = response.usage.total_tokens
        
        logger.info(f"Respuesta generada. Tokens usados: {tokens_used}")
        
        return QuestionResponse(
            answer=answer,
            model=request.model,
            tokens_used=tokens_used
        )
        
    except Exception as e:
        logger.error(f"Error al procesar la pregunta: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

# Endpoint para listar modelos disponibles
@app.get("/models")
async def list_models():
    try:
        models = client.models.list()
        model_list = [model.id for model in models.data]
        return {"available_models": model_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener modelos: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=True  # Solo para desarrollo
    )