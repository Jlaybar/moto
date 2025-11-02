# Initialize the ApifyClient with your API token

import os
import json
import importlib
from pathlib import Path
from typing import Any, Dict, List, Union
from apify_client import ApifyClient



def get_apify_data (marca, modelo, num_paginas=1,  exe=1):
    # Aquí puedes implementar cualquier lógica adicional si es necesario
    # Comprobar si ya existe el archivo data/<marca>/<modelo>.json
    json_path = os.path.join("data", str(marca), f"{modelo}.json")

    if num_paginas==0:
        delete_json_file(marca,modelo,num_paginas)
        exe = 0  
        print(f"⚠️ El archivo {json_path} No tiene datos.")  

    if os.path.isfile(json_path):
        exe = 0
        print(f"✅ El archivo {json_path} ya existe. No se ejecutará el Actor.")

    APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')
    client = ApifyClient(APIFY_API_TOKEN)

    # Generar URLs dinámicamente
    start_urls = []
    for pagina in range(1, num_paginas + 1):
        start_urls.append({
            "url": f"https://motos.coches.net/segunda-mano/{marca}/{modelo}/?pg={pagina}"
        })

    # Prepare the Actor input
    run_input = {
        "browserLog": False,
        "closeCookieModals": False,
        "debugLog": False,
        "downloadCss": False,
        "downloadImages": False,
        "downloadMedia": True,
        "headless": True,
        "ignoreCorsAndCsp": False,
        "ignoreSslErrors": False,
        "injectJQuery": False,
        "keepUrlFragments": False,
        "maxConcurrency": 1,
        "maxRequestRetries": 1,
        "maxRequestsPerCrawl": 1,
        "pageFunction": "async function pageFunction(context) {\n    const { page, request, log } = context;\n    \n    log.info(`Procesando: ${request.url}`);\n    \n    // Esperar a que cargue la página\n    await page.waitForSelector('body', { timeout: 10000 });\n    \n    // Extraer el código fuente HTML completo\n    const htmlContent = await page.content();\n    \n    // Devolver el contenido HTML tal cual\n    return {\n        url: request.url,\n        html: htmlContent,\n        extractedAt: new Date().toISOString()\n    };\n}",
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": [
                "RESIDENTIAL"
            ]
        },
        "respectRobotsTxtFile": False,
        "startUrls": start_urls,
        "useChrome": False,
        "useRequestQueue": False,
        "verboseLog": True
    }

    if exe:
        # Run the Actor and wait for it to finish
        run = client.actor("YJCnS9qogi9XxDgLB").call(run_input=run_input)

        result = []
        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            print(item)
            result.append(item)

        # Ahora 'result' contiene todos los elementos como objetos JSON
        print(f"Se guardaron {len(result)} elementos en la variable 'result'")

        # Crear el directorio si no existe (data/<marca>)
        ruta_directorio = f"data/raw/{marca}"
        os.makedirs(ruta_directorio, exist_ok=True) 
        # Guardar en archivo JSON
        with open(f'data/raw/{marca}/{modelo}.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"✅ Datos guardados en :data/raw/{marca}/{modelo}.json ")


    return 


def get_apify_dict (marca, exe=1):
    # Aquí puedes implementar cualquier lógica adicional si es necesario
    # Comprobar si ya existe el archivo data/<marca>/<modelo>.json

    APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')
 
    client = ApifyClient(APIFY_API_TOKEN)

    # Generar URLs dinámicamente
    start_urls = [ {"url": f"https://motos.coches.net/segunda-mano/{marca}/?pg=1"}]

    # Prepare the Actor input
    run_input = {
        "browserLog": False,
        "closeCookieModals": False,
        "debugLog": False,
        "downloadCss": False,
        "downloadImages": False,
        "downloadMedia": True,
        "headless": True,
        "ignoreCorsAndCsp": False,
        "ignoreSslErrors": False,
        "injectJQuery": False,
        "keepUrlFragments": False,
        "maxConcurrency": 1,
        "maxRequestRetries": 1,
        "maxRequestsPerCrawl": 1,
        "pageFunction": "async function pageFunction(context) {\n    const { page, request, log } = context;\n    \n    log.info(`Procesando: ${request.url}`);\n    \n    // Esperar a que cargue la página\n    await page.waitForSelector('body', { timeout: 10000 });\n    \n    // Extraer el código fuente HTML completo\n    const htmlContent = await page.content();\n    \n    // Devolver el contenido HTML tal cual\n    return {\n        url: request.url,\n        html: htmlContent,\n        extractedAt: new Date().toISOString()\n    };\n}",
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": [
                "RESIDENTIAL"
            ]
        },
        "respectRobotsTxtFile": False,
        "startUrls": start_urls,
        "useChrome": False,
        "useRequestQueue": False,
        "verboseLog": True
    }
    
    json_path = os.path.join("data", str(marca), "tmp.json")
    os.path.join("data", str(marca), f"tmp.json")
    if os.path.isfile(json_path):
        exe = 0
        print(f"✅ El archivo {json_path} ya existe. No se ejecutará el Actor.")
    
    if exe:
        # Run the Actor and wait for it to finish
        run = client.actor("YJCnS9qogi9XxDgLB").call(run_input=run_input)

        result = []
        # Fetch and print Actor results from the run's dataset (if there are any)
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            print(item)
            result.append(item)

        # Ahora 'result' contiene todos los elementos como objetos JSON
        print(f"Se guardaron {len(result)} elementos en la variable 'result'")

        # Crear el directorio si no existe (data/<marca>)
        ruta_directorio = f"data/raw/{marca}"
        os.makedirs(ruta_directorio, exist_ok=True) 
        # Guardar en archivo JSON
        with open(f'data/raw/{marca}/tmp.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"✅ Datos guardados en :data/raw/{marca}/tmp.json ")


    return 


def delete_json_file(marca, modelo, num_paginas):
    import os
    import json

    json_path = os.path.join("data", str(marca), f"{modelo}.json")

    if os.path.isfile(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Verifica que sea una lista y compara longitud con num_paginas
            if isinstance(data, list) and len(data) == num_paginas or num_paginas == 1:
                print(f"✅ El archivo {json_path} ya existe y contiene {len(data)} registros. No se borra.")
            else:
                print(f"⚠️ El archivo {json_path} existe pero contiene {len(data)} registros. Se borra.")
                os.remove(json_path)

        except json.JSONDecodeError:
            print(f"⚠️ El archivo {json_path} no contiene un JSON válido. Se borra.")
            os.remove(json_path)
        except Exception as e:
            print(f"⚠️ Error al procesar {json_path}: {e}")
    else:
        print(f"ℹ️ El archivo {json_path} no existe, nada que borrar.")

    return


def filter_dict(d: dict, text: str) -> dict:
    """
    Busca coincidencias en claves o valores del diccionario.
    Equivale a: SELECT * FROM dict WHERE key LIKE '%palabra1%' AND key LIKE '%palabra2%' ...
    o value LIKE '%palabraX%'
    No distingue mayúsculas/minúsculas.
    """
    # Convertir a minúsculas y dividir en palabras
    palabras = text.lower().split()

    result = {}
    for k, v in d.items():
        k_lower = k.lower()
        v_lower = v.lower()

        # Verifica que TODAS las palabras estén en la clave o en el valor
        if all(p in k_lower or p in v_lower for p in palabras):
            result[k] = v

    print(f"🔎 {len(result)} coincidencia(s) encontradas con '{text}':")
    for k, v in result.items():
        print(f"   • {k} → {v}")

    return result



def load_dict(name_dict: str):
    """
    Carga dinámicamente el diccionario de modelos de una marca desde /dict.
    Ejemplo: load_dict_marca("bmw") → dict_bmw
    """
    module_name = f"dict.dict_{name_dict.lower()}"   # ruta del módulo
    var_name = f"dict_{name_dict.lower()}"    # nombre de la variable dentro

    try:
        # Comprobar si existe el archivo
        file_path = os.path.join("dict", f"dict_{name_dict.lower()}.py")
        if not os.path.exists(file_path):
            print(f"❌ No se encontró el archivo {file_path}")
            return None

        # Importar el módulo dinámicamente
        module = importlib.import_module(module_name)

        # Obtener la variable del módulo
        if hasattr(module, var_name):
            diccionario = getattr(module, var_name)
            print(f"✅ Diccionario '{var_name}' cargado correctamente ({len(diccionario)} elementos).")
            return diccionario
        else:
            print(f"⚠️ El módulo {module_name} no contiene la variable '{var_name}'.")
            return None

    except Exception as e:
        print(f"❌ Error al cargar el diccionario de '{name_dict}': {e}")
        return None


def get_dict_position(data: dict, i: int=0):
# Convertir a listas
    keys = list(data.keys())
    values = list(data.values())
    # Acceder al primer elemento (clave) y su valor
    if len(data) > 0:
        valor = keys[i]
        clave = values[i]
        # print("clave:", clave)
        # print("valor:",valor)
    else:
         clave ='No existe'
         valor = ''
    return  clave, valor 

