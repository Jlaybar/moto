import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 8000))
    
    # Validar que tenemos la API key
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY no encontrada en variables de entorno")