"""Utilidades para cargar datos JSON desde `data/raw`.

Este módulo ofrece funciones para:
- Listar todos los archivos `.json` existentes en un directorio.
- Cargar el contenido de esos `.json` en memoria.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Union


def listar_archivos_json(directorio: str | Path = "data/raw", recursivo: bool = False) -> List[Path]:
    """Devuelve la lista de archivos `.json` en el directorio dado.

    Args:
        directorio: Ruta del directorio a inspeccionar.
        recursivo: Si es True, busca también en subdirectorios.

    Returns:
        Lista de rutas (`Path`) a archivos `.json`.
    """
    base = Path(directorio)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"No se encontró el directorio: {base}")

    patron = "**/*.json" if recursivo else "*.json"
    return [p for p in sorted(base.glob(patron)) if p.is_file()]


def cargar_todos_los_json(rutas: List[str | Path], estricto: bool = True) -> List[Any]:
    """Carga todos los archivos `.json` de un conjunto de rutas.

    Lee cada archivo `.json` provisto en `rutas` y devuelve una lista con
    el contenido parseado de cada uno, en el mismo orden de entrada.

    Args:
        rutas: Lista o conjunto de rutas a archivos `.json`.
        estricto: Si True, lanza excepción ante JSON inválido; de lo
                  contrario, ignora archivos con errores y continúa.

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
                raise ValueError(f"JSON inválido en {ruta}: {e}") from e
            # En modo no estricto, omitimos archivos con error
    return resultados


def extrae_html(
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


 


def _marker_variants(m: str) -> List[str]:
    """Genera variantes del marcador con y sin escapes de comillas.

    Incluye:
    - El marcador tal cual.
    - Versión con `\"` reemplazado por `"`.
    - Versión con `"` reemplazado por `\"`.
    - Versión sin espacios extremos.
    """
    return list({
        m,
        m.strip(),
        m.replace(r'\"', '"'),
        m.replace('"', r'\"'),
    })


def extrae_texto_json_entre(
    contenido_html: Any,
    inicio: str = '"items":[',
    fin: str = '],"totalPages"',
    omitir_vacios: bool = True,
) -> List[str]:
    """Extrae, para cada HTML, el bloque entre `inicio` y `fin` y reconstruye "items": [...].

    Entrada admitida:
    - Lista de dicts con clave "html" (salida de `extrae_html`).
    - Lista de strings.
    - String único.

    Busca `inicio` y `fin` (probando variantes con y sin escapes). Del
    marcador final elimina la subcadena `,"totalPages"` (y su variante
    escapada) para que el resultado quede como "items": [ ... ].

    Args:
        contenido_html: Contenido(s) HTML donde buscar.
        inicio: Marcador inicial. Por defecto '"items":['.
        fin: Marcador final. Por defecto '],"totalPages"'.
        omitir_vacios: Si True, omite entradas sin coincidencias.

    Returns:
        Lista de strings con fragmentos reconstruidos "items": [...].
    """

    # Normalizar a lista de textos
    textos: List[str] = []
    if isinstance(contenido_html, str):
        textos = [contenido_html]
    elif isinstance(contenido_html, list):
        for elem in contenido_html:
            if isinstance(elem, dict) and "html" in elem and isinstance(elem["html"], str):
                textos.append(elem["html"])
            elif isinstance(elem, str):
                textos.append(elem)
            elif isinstance(elem, list):
                for sub in elem:
                    if isinstance(sub, dict) and "html" in sub and isinstance(sub["html"], str):
                        textos.append(sub["html"])
                    elif isinstance(sub, str):
                        textos.append(sub)
    elif isinstance(contenido_html, dict) and "html" in contenido_html and isinstance(contenido_html["html"], str):
        textos = [contenido_html["html"]]

    resultados: List[str] = []

    for contenido in textos:
        inicio_idx: int | None = None
        inicio_literal: str | None = None
        for variante in _marker_variants(inicio):
            pos = contenido.find(variante)
            if pos != -1:
                inicio_literal = variante
                inicio_idx = pos + len(variante)
                break

        if inicio_idx is None:
            if not omitir_vacios:
                resultados.append("")
            continue

        fin_idx: int | None = None
        fin_literal: str | None = None
        for variante in _marker_variants(fin):
            pos = contenido.find(variante, inicio_idx)
            if pos != -1:
                fin_idx = pos
                fin_literal = variante
                break

        if fin_idx is None or fin_literal is None:
            if not omitir_vacios:
                resultados.append("")
            continue

        cuerpo = contenido[inicio_idx:fin_idx]

        # Limpiar ,"totalPages" o su versión escapada del marcador final
        fin_limpio = fin_literal.replace(',"totalPages"', "").replace(',\\"totalPages\\"', "")

        fragmento = f"{inicio_literal}{cuerpo}{fin_limpio}"
        resultados.append(fragmento)

    return resultados


def main() -> None:
    """Ejecución ad-hoc: carga JSON por rutas y extrae `items`."""
    try:
        archivos = listar_archivos_json("data/raw", recursivo=False)
    except FileNotFoundError as e:
        print(e)
        return

    print(f"Encontrados {len(archivos)} archivo(s) .json en data/raw")
    contenidos = cargar_todos_los_json(archivos, estricto=False)
    print(f"Cargados {len(contenidos)} archivo(s) JSON")


if __name__ == "__main__":
    main()
