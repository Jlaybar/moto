"""Compatibilidad: reexporta recursos desde `catalog`.

Preferir `from catalog.dict_prov import dict_prov`.
"""

from catalog.dict_prov import dict_prov  # reexport

__all__ = [
    "dict_prov",
]
