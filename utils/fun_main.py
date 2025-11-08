import os
from .fun_get_data import *
from .fun_parse_data import *
from .fun_dict import *

from .fun_plot import *
from .fun_db import *

from .fun_model_index import save_models_index

def get_moto_marca  (MARCA: str, 
                     path_dict:str='dict/moto/',
                     path_data:str='data/moto/raw'
                     ):
    # -----------------------------------------------------------------
    # Paso 01-  Creamos el Dicionario  MARCA y MODELOS
    #-----------------------------------------------------------------
    # Extracion de la marca
    print(f'ℹ️ Paso 01-  Extraccion del marca')
    marca='No existe'
    dict_marca = load_dict('marca', path_dict)
    dict_marca_filter =filter_dict(dict_marca, MARCA)
    marca, marca_des = get_dict_position(dict_marca_filter)

    if marca =='No existe' :
        print(f"❌ El marca: {MARCA} no exite ")
        return marca
        
    get_moto_apify_dict (marca, path_data)
    get_dict_marca (marca, path_dict, path_data)

    print(f"✅marca: {marca} ")

    return marca 

def get_moto_modelo (marca: str, 
                     MODELO: str, 
                     path_dict:str='dict/moto/'
                    ):
    
    # Extracion del modelo 
    print(f'ℹ️ Paso 01-  Extraccion del modelo')
    modelo='No existe'
    if marca =='No existe' :
        print(f"❌ No se puede encontrar el  modelo: {MODELO} ")
        return
    dict_modelo = load_dict(marca, path_dict)
    dict_modelo_filter =filter_dict(dict_modelo, MODELO)
    modelo, modelo_des = get_dict_position(dict_modelo_filter)

    if modelo =='No existe' :
        print(f"❌ El modelo: {MODELO} no exite")
        return modelo
    
    print(f"✅modelo: {modelo} ")
    return modelo



def get_moto_data (marca: str, 
                   modelo: str, 
                   path_data:str,
                   delete_json:int=0
                   ):
    # -----------------------------------------------------------------
    # Paso 02-  Extraccion de cadena HTML
    #-----------------------------------------------------------------
    print(f'ℹ️ Paso 02-  Extraccion de cadena HTML')
    json_dir = os.path.join(path_data, marca)
    os.makedirs(json_dir, exist_ok=True)

    json_path = os.path.join(json_dir, f"{modelo}.json")

    # Borrado opcional del modelo JSON (seguro)
    if delete_json:
        if os.path.isfile(json_path):
            try:
                os.remove(json_path)
                print(f"🗑️ Se borra el archivo: {json_path}")
            except Exception as e:
                print(f"❌ No se pudo borrar {json_path}: {e}")
        else:
            print(f"ℹ️ El archivo {json_path} no existe. Nada que borrar.")

    # URL informativa (pg=1)
    print(f"https://motos.coches.net/segunda-mano/{marca}/{modelo}/?pg=1")

    # Si todavía existe el JSON, no sobrescribimos
    if os.path.exists(json_path):
        print(f"⚠️ Los datos {marca}:{modelo} ya existen en: {json_path}. No se sobrescribe.")
    else:
        # 1) Extraer la primera página
        get_moto_apify_data(marca, modelo, 1, path_data)

        # 2) Obtener número total de páginas
        num_paginas = get_num_pages(marca, modelo,path_data)
        if not isinstance(num_paginas, int) or num_paginas < 1:
            print(f"⚠️ num_paginas inválido ({num_paginas}). Continúo solo con la primera página.")
            num_paginas = 1

        # 3) Borrado condicional del JSON si no cumple tu regla interna
        delete_json_file(marca, modelo, num_paginas,path_data)

        # 4) Extraer todas las páginas del modelo
        get_moto_apify_data(marca, modelo, num_paginas,path_data)

        return
    

def get_moto_items_json (marca:str,
                         modelo:str,
                         path_data:str
                         ) :

    # -----------------------------------------------------------------
    # Paso 03 -  Parseo de los datos 
    #-----------------------------------------------------------------
    items_json=[]
    print(f'ℹ️ Paso 03 - Parseo de los datos ')
    #carga de datos json
    path_row = f"{path_data}/{marca}/{modelo}"

    p = Path(path_row)
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

    print(f"✅Cargados {len(files_json)} archivo(s) JSON")
    #-------------------------------------------------------------------
    content_json= read_json_files(files_json, estricto=False)

    if len(content_json)==0:
        print(f"❌No hay datos en el JSON")
        return
    content_html = get_html_from_json(content_json)

    if len(content_html)==0:
        print(f"❌No hay contendio HTML")
        delete_json_file(marca, modelo, 0, path_data)
        return
    
    content_items = get_txt_between_from_html(content_html,
                                              ini_text='"items":[{"bodyTypeId":',
                                              fin_text='}],"totalPages"'
                                              )
    if len(content_items)==0:
        print(f"❌No hay contendio ITEM")
        return
    items_json = get_parse_item(content_items , extrac_list=EXTRACT_LIST)

    if len(items_json)==0:
        print(f"❌No hay contendio PARSE")
        return
    else:
        items_json = remove_duplicates_from_json(items_json)
        print(f'ℹ️ Paso 03 - Insercion de db ')
        insert_motos_from_json(items_json, marca,modelo)

    return items_json


def get_moto_model_json (marca:str,
                         modelo:str,
                         items_json,
                         path_model:str
                         ) :

    # -----------------------------------------------------------------
    # Paso 04 - Guardar model_json en data/model/marca/modelo.json
    #-----------------------------------------------------------------
    print(f'ℹ️ Paso 04 -  Guardado model.json')
    from utils.fun_model import modelfit
    model_json = modelfit(items_json, marca, modelo)
    model_json = remove_duplicates_from_json(model_json)

    if 'model_json' in locals():
        save_dir = os.path.join(path_model, marca)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"{modelo}.json")
        import json
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(model_json, f, ensure_ascii=False, indent=2)
        print(f"Guardado model_json en: {save_path}")

    return model_json




def get_moto_elasticity (MARCA: str,
                        MODELO: str, 
                        path_dict:str='dict/moto/',
                        path_data:str='data/moto/raw',
                        path_model:str='data/moto/model',
                        delete_json: int = 0):
    
    # -----------------------------------------------------------------
    # Paso 01-  Creamos el Dicionario  
    #-----------------------------------------------------------------
    # Extracion de la marca
    
    marca  = get_moto_marca  (MARCA,path_dict,path_data)

    modelo = get_moto_modelo (marca, MODELO, path_dict)

    if (marca =='No existe') or (modelo =='No existe'):
        return

    path_modelo=f'{path_model}/{marca}/{modelo}.json'

    if not os.path.exists(path_modelo):

        print(f"❌ No se encontró el archivo JSON en la ruta: {path_modelo}")
        # -----------------------------------------------------------------
        # Paso 02-  Extraccion de cadena HTML
        #-----------------------------------------------------------------
        get_moto_data (marca, modelo, path_data, delete_json)
            
        # -----------------------------------------------------------------
        # Paso 03 -  Parseo de los datos 
        #-----------------------------------------------------------------
        items_json = get_moto_items_json ( marca,modelo, path_data )
        if len(items_json)==0:
            return
        # ----------------------------------------------------------------
        # Paso 04 - Guardar model_json en data/model/marca/modelo.json
        #-----------------------------------------------------------------
        model_json = get_moto_model_json(marca, modelo,items_json,path_model) 
        if len(model_json)==0:
            return
        # ----------------------------------------------------------------
        # Paso 05 - creacion del indice de modelos
        #-----------------------------------------------------------------
        save_models_index(f'{path_model}/models_index.json', path_model)
    
    else: 
        print(f"✅ El archivo JSON existe en  la ruta: {path_modelo}")

    return 
