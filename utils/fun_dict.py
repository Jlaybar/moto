import os
import importlib
from pathlib import Path
from typing import Any, Dict


def filter_dict(d: Dict[str, str], text: str) -> Dict[str, str]:
    """
    Busca coincidencias en claves o valores del diccionario.
    Equivale a: SELECT * FROM dict WHERE key LIKE '%p1%' AND key LIKE '%p2%' ...
    (o value LIKE '%px%'). Búsqueda case-insensitive.
    """
    palabras = text.lower().split()
    result: Dict[str, str] = {}
    for k, v in d.items():
        k_lower = str(k).lower()
        v_lower = str(v).lower()
        if all(p in k_lower or p in v_lower for p in palabras):
            result[k] = v
    print(f"➡ {len(result)} coincidencia(s) con '{text}'")
    for k, v in result.items():
        print(f"   - {k} -> {v}")
    return result


def _normalize_base_to_module(base: str) -> str:
    """Normaliza base tipo ruta o paquete a módulo: 'dict.source.<dominio>'"""
    b = (base or "").strip().strip("/\\.")
    if "/" in b or "\\" in b:
        b = b.replace("\\", "/").replace("/", ".")
    if not b:
        b = "dict.source.moto"
    if not b.startswith("dict"):
        b = f"dict.source.{b}"
    if b.startswith("dict.") and not b.startswith("dict.source."):
        parts = b.split(".")
        if len(parts) >= 2 and parts[1] != "source":
            parts.insert(1, "source")
            b = ".".join(parts)
    return b


def load_dict_(name_dict: str, path:str) -> Any:
    """
    Carga un diccionario por nombre y base (compat con llamadas anteriores).
    - name_dict: por ejemplo "bmw" -> busca variable `dict_bmw`.
    - base: "dict/source/moto", "dict.source.moto" o solo "moto".
    """
    module_base = _normalize_base_to_module(path)
    module_name = f"{module_base}.dict_{name_dict.lower()}"
    var_name = f"dict_{name_dict.lower()}"

    try:
        module = importlib.import_module(module_name)
        if hasattr(module, var_name):
            diccionario = getattr(module, var_name)
            print(f"✅ Diccionario '{var_name}' cargado ({len(diccionario)} elementos)")
            return diccionario
        else:
            print(f"⚠ El módulo {module_name} no contiene la variable '{var_name}'")
            return None
    except Exception as e:
        # Mensaje claro; normalmente ocurre si la ruta base o el nombre no son correctos
        print(f"⛔ Error al cargar '{name_dict}' desde '{module_base}': {e}")
        return None


def load_dict(name_dict: str, path: str) -> Any:
    """Compatibilidad con firma anterior: redirige a load_dict_."""
    return load_dict_(name_dict, base=path)


def get_dict_position(data: Dict[str, str], i: int = 0):
    keys = list(data.keys())
    values = list(data.values())
    if len(data) > 0 and 0 <= i < len(data):
        clave = values[i]
        valor = keys[i]
    else:
        clave = "No existe"
        valor = ""
    return clave, valor


__all__ = ["filter_dict", "load_dict", "load_dict_", "get_dict_position"]


