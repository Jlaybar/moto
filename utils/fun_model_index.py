from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List


def build_models_index(root: str | os.PathLike = "data/model") -> List[Dict[str, object]]:
    """
    Recorre el árbol de `root` con estructura esperada `root/<marca>/*.json`
    y devuelve una lista de objetos: [{"marca": <marca>, "modelos": [<modelo>, ...]}, ...]

    - <marca>: nombre del directorio (en minúsculas)
    - <modelo>: nombre del fichero sin extensión (tal cual, respetando guiones y guiones bajos)
    """
    root_path = Path(root)
    if not root_path.exists():
        return []

    index: List[Dict[str, object]] = []

    for brand_dir in sorted([p for p in root_path.iterdir() if p.is_dir()]):
        marca = brand_dir.name.lower()
        modelos: List[str] = []
        for jf in sorted(brand_dir.glob("*.json")):
            modelos.append(jf.stem)
        if modelos:
            index.append({"marca": marca, "modelos": modelos})

    return index


def save_models_index(out_path: str | os.PathLike, root: str | os.PathLike = "data/model") -> Path:
    """
    Construye el índice y lo guarda como JSON pretty en `out_path`.
    Devuelve la ruta escrita.
    """
    data = build_models_index(root)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Genera índice de marcas/modelos desde data/model")
    parser.add_argument("--root", default="data/model", help="Directorio raíz (por defecto data/model)")
    parser.add_argument("--out", default="data/models_index.json", help="Fichero de salida JSON")
    args = parser.parse_args()

    path = save_models_index(args.out, args.root)
    print(f"Índice generado: {path}")

