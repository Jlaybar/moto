import os
import runpy
from typing import Dict


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


def load_dict(name_dict: str, path_dict: str):
    file_path = os.path.join(path_dict, f"dict_{name_dict.lower()}.py")
    try:
        data = runpy.run_path(file_path)
        var_name = f"dict_{name_dict.lower()}"
        diccionario = data.get(var_name)
        if diccionario is not None:
            print(f"✅ Diccionario '{var_name}' cargado ({len(diccionario)} elementos)")
            return diccionario
        else:
            print(f"⚠ No se encontró la variable '{var_name}' en {file_path}")
            return None
    except Exception as e:
        print(f"⛔ Error al cargar el archivo '{file_path}': {e}")
        return None



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


__all__ = ["filter_dict", "load_dict",  "get_dict_position"]


