import requests
import json

def test_backend():
    print("=== Probando Backend ChatGPT ===")
    
    # URL de tu servidor
    url = "http://localhost:8000/ask"
    
    # Datos de la petición
    data = {
        "question": "Explique qué es la inteligencia artificial en 50 palabras",
        "model": "gpt-3.5-turbo",
        "max_tokens": 300,
        "temperature": 0.7
    }
    
    try:
        print("1. Enviando pregunta al servidor...")
        
        # Hacer la petición POST
        response = requests.post(url, json=data)
        
        print(f"2. Código de respuesta: {response.status_code}")
        
        # Verificar si la petición fue exitosa
        if response.status_code == 200:
            result = response.json()
            print("✅ ¡Petición exitosa!")
            print(f"📊 Modelo usado: {result['model']}")
            print(f"🔢 Tokens utilizados: {result['tokens_used']}")
            print(f"💬 Respuesta:\n{result['answer']}")
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Detalles: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Error: No se puede conectar al servidor")
        print("   Asegúrate de que el servidor esté ejecutándose en http://localhost:8000")
    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")

if __name__ == "__main__":
    test_backend()
