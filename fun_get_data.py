# Initialize the ApifyClient with your API token
from apify_client import ApifyClient
import os
import json



def fun_get_data (marca, modelo, num_paginas, exe=1):
    # Aquí puedes implementar cualquier lógica adicional si es necesario
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

        # Crear el directorio si no existe
        ruta_directorio = f"data/raw/{marca}"
        os.makedirs(ruta_directorio, exist_ok=True) 
        # Guardar en archivo JSON
        with open(f'data/{marca}/{modelo}.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"Datos guardados en :data/{marca}/{modelo}.json ")


    return 


