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
#        Moto: construir/actualizar modelo (elasticidad)
# ----------------------------------------------------
try:
    from utils.fun_main import get_moto_elasticity, get_moto_marca_modelo
except Exception:
    get_moto_elasticity = None
    get_moto_marca_modelo = None

class MotoElasticityIn(BaseModel):
    marca: str
    modelo: str
    delete_json: int | bool = 0
    # Permitir rutas personalizadas opcionales
    path_dict: str | None = None
    path_data: str | None = None
    path_model: str | None = None

@app.post("/api/moto/elasticity")
async def api_moto_elasticity(payload: MotoElasticityIn):
    if get_moto_elasticity is None or get_moto_marca_modelo is None:
        raise HTTPException(status_code=500, detail="Módulo moto no disponible")

    # Rutas por defecto dentro del repo
    path_dict = payload.path_dict or os.path.join("dict", "moto")
    path_data = payload.path_data or os.path.join("data", "moto", "raw")
    path_model = payload.path_model or os.path.join("data", "moto", "model")

    try:
        # Resolver slugs de marca/modelo antes de ejecutar
        marca_slug, modelo_slug = get_moto_marca_modelo(payload.marca, payload.modelo, path_dict, path_data)
        if marca_slug == 'No existe' or modelo_slug == 'No existe':
            raise HTTPException(status_code=404, detail="Marca o modelo no encontrados en diccionario")

        # Ejecutar pipeline (descarga → parseo → model_json → índice)
        get_moto_elasticity(payload.marca, payload.modelo, path_dict, path_data, path_model, int(bool(payload.delete_json)))

        # Comprobar resultado
        model_path = os.path.join(path_model, marca_slug, f"{modelo_slug}.json")
        exists = os.path.exists(model_path)
        return {
            "ok": True,
            "marca": payload.marca,
            "modelo": payload.modelo,
            "marca_slug": marca_slug,
            "modelo_slug": modelo_slug,
            "model_path": model_path,
            "exists": exists,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Fallo en /api/moto/elasticity")
        raise HTTPException(status_code=500, detail=f"Error al construir modelo: {e}")

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
