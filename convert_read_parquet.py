import pandas as pd
import requests
from io import BytesIO

column_names = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','codmes','pk_id_orden','fk_id_orden_tipo','vc_orden_ruc','vc_orden_periodo','vc_orden_numero','vc_orden_numero_siaf','dt_orden_fecha','dc_orden_monto','vc_orden_proveedor','vc_orden_descripcion','dt_orden_fec_reg','in_orden_anno','in_orden_mes']
dtypes = {
    'tipo_poder_id': 'int64',
    'tipo_poder_nombre': 'object',
    'categoria': 'object',
    'entidad_id': 'int64',
    'entidad_nombre': 'object',
    'codmes': 'object',
    'pk_id_orden': 'int64',
    'fk_id_orden_tipo': 'int64',
    'vc_orden_ruc': 'int64',
    'vc_orden_periodo': 'object',
    'vc_orden_numero': 'object',
    'vc_orden_numero_siaf': 'object',
    'dt_orden_fecha': 'object',
    'dc_orden_monto': 'float64',
    'vc_orden_proveedor': 'object',
    'vc_orden_descripcion': 'object',
    'dt_orden_fec_reg': 'object',
    'in_orden_anno': 'int64',
    'in_orden_mes': 'int64'
}

def txt_to_parquet(txt_file, parquet_file, column_names, dtypes):
    df = pd.read_csv(txt_file, sep='\t', names=column_names, dtype=dtypes, header=0)
    df = df.drop(['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','pk_id_orden','fk_id_orden_tipo','vc_orden_periodo','vc_orden_numero','vc_orden_numero_siaf','dt_orden_fecha','dt_orden_fec_reg','in_orden_anno','in_orden_mes'], axis=1)
    df.to_parquet(parquet_file, engine='pyarrow')
    print(f"Conversion successful! The Parquet file has been saved as: {parquet_file}")

path = "./data"
txt_file = f"{path}/4_ordenes_de_servicio_2023.txt"
parquet_file = f"{path}/4_ordenes_de_servicio_2023.parquet"

txt_to_parquet(txt_file, parquet_file, column_names, dtypes)

def read_parquet(url):
    response = requests.get(url)
    if response.status_code == 200:
        parquet_file = BytesIO(response.content)
        df = pd.read_parquet(parquet_file, engine='pyarrow')
        return df
    print(f"Failed to fetch the file. HTTP Status Code: {response.status_code}")

df_2019 = read_parquet('https://github.com/a20121248/mineria-web-trabajo-parcial/raw/refs/heads/main/4_ordenes_de_servicio_2019.parquet')
df_2020 = read_parquet('https://github.com/a20121248/mineria-web-trabajo-parcial/raw/refs/heads/main/4_ordenes_de_servicio_2020.parquet')
df_2021 = read_parquet('https://github.com/a20121248/mineria-web-trabajo-parcial/raw/refs/heads/main/4_ordenes_de_servicio_2021.parquet')
df_2022 = read_parquet('https://github.com/a20121248/mineria-web-trabajo-parcial/raw/refs/heads/main/4_ordenes_de_servicio_2022.parquet')
df_2023 = read_parquet('https://github.com/a20121248/mineria-web-trabajo-parcial/raw/refs/heads/main/4_ordenes_de_servicio_2023.parquet')
