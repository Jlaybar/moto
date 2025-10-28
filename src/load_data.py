"""Utilidades para cargar datos JSON desde `data/raw`.

Este módulo ofrece funciones para:
- Listar todos los files_json `.json` existentes en un directorio.
- Cargar el contenido de esos `.json` en memoria.
"""

from __future__ import annotations

import json
from typing import List, Union
from pathlib import Path
from typing import Any, Dict, List, Union


BASE_URL = "https://motos.coches.net/"
EXTRACT_LIST = ['title','km', 'price', 'year','url','imgUrl','provinceId','hp']


def list_json_flies(PATH_ROW, recursivo: bool = False) -> List[Path]:  
    """Devuelve la lista de files_json `.json` en el directorio dado.

    Args:
        directorio: Ruta del directorio a inspeccionar.
        recursivo: Si es True, busca también en subdirectorios.

    Returns:
        Lista de rutas (`Path`) a files_json `.json`.
    """
    base = Path(PATH_ROW)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"No se encontro el directorio: {base}")

    patron = "**/*.json" if recursivo else "*.json"
    return [p for p in sorted(base.glob(patron)) if p.is_file()]


def read_json_files(rutas: List[str | Path], estricto: bool = True) -> List[Any]:
    """Carga todos los files_json `.json` de un conjunto de rutas.

    Lee cada archivo `.json` provisto en `rutas` y devuelve una lista con
    el contenido parseado de cada uno, en el mismo orden de entrada.

    Args:
        rutas: Lista o conjunto de rutas a files_json `.json`.
        estricto: Si True, lanza excepción ante JSON inválido; de lo
                  contrario, ignora files_json con errores y continúa.

    Returns:
        Lista con el contenido JSON de cada archivo.

    Nota:
        No se devuelve un `set` porque los objetos JSON (dict/list) no son
        hashables. Si necesitas eliminar duplicados, puedes convertir cada
        elemento a cadena con `json.dumps(..., sort_keys=True)` y operar allí.
    """
    resultados: List[Any] = []
    for r in rutas:
        ruta = Path(r)
        if not ruta.exists() or not ruta.is_file():
            raise FileNotFoundError(f"No existe el archivo: {ruta}")
        if ruta.suffix.lower() != ".json":
            continue
        try:
            with ruta.open("r", encoding="utf-8") as f:
                datos = json.load(f)
            resultados.append(datos)
        except json.JSONDecodeError as e:
            if estricto:
                raise ValueError(f"JSON invǭlido en {ruta}: {e}") from e
            # En modo no estricto, omitimos files_json con error
    return resultados

def get_html_from_json(
    datos_json: Any,
    clave: str = "html",
    omitir_vacios: bool = True,
) -> List[Dict[str, Any]]:
    """Extrae solo el campo HTML de una estructura JSON agrupada por archivo.

    Acepta estructuras como:
        { "archivo.json": [ {"url":..., "html":..., ...}, ... ] }
    y devuelve:
        [ {"html": ...}, {"html": ...}, ... ]

    También funciona si `datos_json` es una lista (o lista de listas) de
    registros tipo dict. Recorre recursivamente y extrae cualquier dict que
    contenga la clave indicada.

    Args:
        datos_json: Estructura con registros que incluyen la clave de HTML.
        clave: Nombre de la clave a extraer (por defecto "html").
        omitir_vacios: Si True, omite valores vacíos o None.

    Returns:
        Lista de diccionarios con solo la clave de HTML.
    """
    resultados: List[Dict[str, Any]] = []

    def visitar(nodo: Any) -> None:
        if isinstance(nodo, dict):
            if clave in nodo:
                valor = nodo.get(clave)
                if not (
                    omitir_vacios
                    and (
                        valor is None
                        or (isinstance(valor, str) and valor.strip() == "")
                    )
                ):
                    resultados.append({clave: valor})
            for v in nodo.values():
                visitar(v)
        elif isinstance(nodo, list):
            for elem in nodo:
                visitar(elem)
        # otros tipos se ignoran

    visitar(datos_json)
    return resultados


# Redefinición: get_txt_between_from_html para trabajar sobre contenido_html
def get_txt_between_from_html(
    contenido_html: Any,
    ini_text: str ='"items":[{"bodyTypeId":',
    fin_text: str = '}],"totalPages"'
) -> List[str]:
    """Extrae, para cada HTML, el bloque entre `inicio` y `fin` y reconstruye \"items\": [...].

    Entrada admitida: lista de dicts con clave 'html', lista de strings o string único.
    Del marcador final elimina ,\"totalPages\" (o su versión escapada) para formar "items": [...].
    """
    textos: List[str] = []
    if isinstance(contenido_html, str):
        textos = [contenido_html]
    elif isinstance(contenido_html, list):
        for elem in contenido_html:
            if isinstance(elem, dict) and 'html' in elem and isinstance(elem['html'], str):
                textos.append(elem['html'])
            elif isinstance(elem, str):
                textos.append(elem)
            elif isinstance(elem, list):
                for sub in elem:
                    if isinstance(sub, dict) and 'html' in sub and isinstance(sub['html'], str):
                        textos.append(sub['html'])
                    elif isinstance(sub, str):
                        textos.append(sub)
    elif isinstance(contenido_html, dict) and 'html' in contenido_html and isinstance(contenido_html['html'], str):
        textos = [contenido_html['html']]

    resultados: List[str] = []
    for contenido in textos:
        contenido = contenido.replace("\\", "")  
        ini = contenido.find(ini_text)
        if ini == -1:
            return ""
        ini += len(ini_text)
        fin = contenido.find(fin_text, ini)
        if fin == -1:
            return ""
        cuerpo = contenido[ini:fin]
        fin_text= fin_text.replace(',\"totalPages\"', '')
        fragmento = f"{ini_text}{cuerpo}{fin_text}"
        resultados.append(fragmento)
    return resultados
def _find_json_array_after_items(s: str) -> str:
    key = '"items":['
    start = s.find(key)
    if start == -1:
        return ""
    i = start + len(key) - 1
    depth = 0
    arr_start = None
    for pos in range(i, len(s)):
        ch = s[pos]
        if ch == '[':
            if depth == 0:
                arr_start = pos
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0 and arr_start is not None:
                return s[arr_start:pos+1]
    return ""

def _normalize_url(url: str) -> str:
    if not url:
        return url
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    # Evita doble barra al unir
    if u.startswith("/"):
        u = u[1:]
    return BASE_URL + u

def get_parse_item(extrae_items: Union[str, List[str]], extrac_list: List[str] = None) -> List[dict]:
    if extrac_list is None:
        extrac_list = ['km', 'precio', 'year']

    norm_map = {
        'km': 'km',
        'precio': 'price',
        'price': 'price',
        'year': 'year',
        'año': 'year',
        'anio': 'year',
        'url': 'url',
    }
    wanted = [norm_map.get(f.lower(), f) for f in extrac_list]

    chunks = [extrae_items] if isinstance(extrae_items, str) else list(extrae_items)
    resultados = []

    for chunk in chunks:
        if not isinstance(chunk, str):
            continue
        arr_text = _find_json_array_after_items(chunk)
        if not arr_text:
            continue

        try:
            items = json.loads(arr_text)
            if not isinstance(items, list):
                continue
        except json.JSONDecodeError:
            continue

        for obj in items:
            if not isinstance(obj, dict):
                continue
            row = {}

            # siempre útil añadir id si está
            if 'id' in obj:
                row['id'] = obj.get('id')

            # añade url siempre que exista, normalizada
            if 'url' in obj:
                row['url'] = _normalize_url(obj.get('url'))

            for f in wanted:
                val = obj.get(f, None)

                if f == 'url':
                    row['url'] = _normalize_url(val)
                    continue

                if isinstance(val, str) and f in ('km', 'price', 'year'):
                    num = ''.join(ch for ch in val if ch.isdigit())
                    val = int(num) if num else None

                row[f] = val

            resultados.append(row)

    return resultados

def get_items_json (PATH_ROW) -> List[Any]:
    """Ejecución ad-hoc: carga JSON por rutas y extrae `items`."""
    p = Path(PATH_ROW)
    files_json = []
    if p.is_file() or p.suffix.lower() == '.json':
        if p.suffix.lower() != '.json':
            p = p.with_suffix('.json')
        files_json = [p]
    elif p.is_dir():
        files_json = list_json_flies(p, recursivo=False)
    else:
        candidate = p.with_suffix('.json')
        if candidate.exists():
            files_json = [candidate]
        else:
            print('Ruta no valida')
            return []

    print(f"Cargados {len(files_json)} archivo(s) JSON")

    content_json= read_json_files(files_json)
    print(f"Extaidos {len(content_json)} contenidos JSON")

    content_html = get_html_from_json(content_json)
    print(f"Extaidos {len(content_html)} contenidos HTML")

    content_items = get_txt_between_from_html(content_html)
    print(f"Extaidos  {len(content_items)} contenidos ITMEMS")

    items_json = get_parse_item(content_items , extrac_list=EXTRACT_LIST)

    print(f"Extaidos  {len(items_json)} contenidos ITMEMS")

    return items_json


#if __name__ == "__main__":
#    main()
