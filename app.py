from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
import logging
from config import Config
from datetime import datetime
from fastapi.staticfiles import StaticFiles
from pydantic import Field
import re
import os

# Helpers para mensajes
try:
    from utils.fun_mensaje import ensure_schema, insert_mensaje, EMAIL_RE
except Exception:
    # Si no existe el helper aún, definimos un regex básico para no romper import
    ensure_schema = None
    insert_mensaje = None
    EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

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


# Modelo de entrada para mensajes
class MensajeIn(BaseModel):
    correo: str
    asunto: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=5000)

# Endpoint de salud
@app.get("/")
async def root():
    return {"message": "ChatGPT API Backend funcionando correctamente"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "chatgpt-backend"}


# Montar estáticos de la carpeta public en /public
app.mount("/public", StaticFiles(directory="public"), name="public")


# Inicialización de base de datos en arranque
@app.on_event("startup")
async def _init_db():
    try:
        if ensure_schema:
            ensure_schema()
    except Exception as e:
        logger.warning(f"No se pudo inicializar la base de datos: {e}")


# Endpoint para crear mensajes (SQLite)
@app.post("/api/mensajes")
async def crear_mensaje(payload: MensajeIn, request: Request):
    if not EMAIL_RE.match(payload.correo.strip()):
        raise HTTPException(status_code=400, detail="Correo no válido.")

    ip = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if insert_mensaje is None:
        raise HTTPException(status_code=500, detail="Módulo de base de datos no disponible.")

    new_id = insert_mensaje(payload.correo.strip(), payload.asunto.strip(), payload.body.strip(), ip, created_at)
    return {"ok": True, "id": new_id, "created_at": created_at}


@app.get("/api/health")
async def api_health():
    return {"ok": True}

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
@app.get("/models/chatgpt")
async def list_models():
    try:
        models = client.models.list()
        model_list = [model.id for model in models.data]
        return {"available_models": model_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener modelos: {str(e)}")

# ----------------------------------------------------
#        Moto: elasticidad (GET simplificado)
# ----------------------------------------------------
try:
    from utils.fun_main import get_moto_elasticity as moto_elasticity_run
except Exception:
    moto_elasticity_run = None

@app.get("/api/moto/elasticiadad")
async def api_moto_elasticidad(marca: str, modelo: str, delete_json: int = 0):
    if moto_elasticity_run is None:
        raise HTTPException(status_code=500, detail="Módulo moto no disponible")
    try:
        # Ejecuta pipeline de elasticidad con rutas por defecto
        moto_elasticity_run(marca, modelo, int(delete_json))
        # Comprobar existencia del JSON resultante en rutas por defecto
        path_model = os.path.join("data", "moto", "model")
        model_path = os.path.join(path_model, marca, f"{modelo}.json")
        exists = os.path.exists(model_path)
        return {
            "ok": True,
            "marca": marca,
            "modelo": modelo,
            "delete_json": int(delete_json),
            "model_path": model_path,
            "exists": exists,
        }
    except Exception as e:
        logger.exception("Fallo en /api/moto/elasticiadad")
        raise HTTPException(status_code=500, detail=f"Error al construir modelo: {e}")

# ----------------------------------------------------
#        Moto: resolver marca y modelo
# ----------------------------------------------------
try:
    from utils.fun_main import get_moto_marca, get_moto_modelo
except Exception:
    get_moto_marca = None
    get_moto_modelo = None

@app.get("/api/moto/marca")
async def api_moto_marca(MARCA: str):
    if get_moto_marca is None:
        raise HTTPException(status_code=500, detail="Módulo moto no disponible")
    try:
        marca = get_moto_marca(MARCA)
        return {"input": MARCA, "marca": marca}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resolviendo marca: {e}")

@app.get("/api/moto/modelo")
async def api_moto_modelo(marca: str, MODELO: str):
    if get_moto_modelo is None:
        raise HTTPException(status_code=500, detail="Módulo moto no disponible")
    try:
        modelo = get_moto_modelo(marca, MODELO)
        return {"marca": marca, "input": MODELO, "modelo": modelo}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resolviendo modelo: {e}")

 

if __name__ == "__main__":
    import uvicorn
    # Garantiza que la tabla exista al iniciar en modo script
    try:
        if ensure_schema:
            ensure_schema()
    except Exception:
        pass
    uvicorn.run(
        "app:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=True  # Solo para desarrollo
    )
