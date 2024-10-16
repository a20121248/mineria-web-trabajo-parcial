import scrapy
import pandas as pd
from ..items import ViaticoPasajeItem
from datetime import datetime
from dateutil.relativedelta import relativedelta

class OrdenServicioSpider(scrapy.Spider):
    name = 'viaticos_pasajes'
    allowed_domains = ['transparencia.gob.pe']
    personal_URL = 'http://www.transparencia.gob.pe/contrataciones/pte_transparencia_contrataciones_genera.aspx?tipo_viaje=1&modo_viatico=1&Ver=&tipo_seleccion=2&'
    entidades_file = 'data/entidades.txt'

    def start_requests(self):
        entidades_df = pd.read_csv(self.entidades_file, sep='\t', encoding='utf8')

        start_date = datetime.strptime(self.periodo_ini, '%Y%m')
        end_date = datetime.strptime(self.periodo_fin, '%Y%m')
        current_date = start_date
        while current_date <= end_date:
            periodo = current_date.strftime('%Y%m')
            anho = periodo[:4]
            mes = periodo[-2:]
            codmes = f'{anho}-{mes}'

            current_date += relativedelta(months=1)

            for entidad_idx, entidad in entidades_df.iterrows():
                meta = {
                    'tipo_poder_id': entidad['tipo_poder_id'],
                    'tipo_poder_nombre': entidad['tipo_poder_nombre'],
                    'categoria': entidad['categoria'],
                    'entidad_id': entidad['entidad_id'],
                    'entidad_nombre': entidad['entidad_nombre'],
                    'codmes': codmes,
                }

                file_url = f"{self.personal_URL}id_entidad={entidad['entidad_id']}&in_anno={anho}&in_mes={mes}"
                self.logger.info(f"Procesando spider: {self.name}, entidad: {entidad['entidad_nombre']}(id: {entidad['entidad_id']}), periodo: {codmes}', url: {file_url}")
                yield scrapy.Request(url=file_url, meta=meta, callback=self.parse_viatico_pasaje)
                break

    def parse_viatico_pasaje(self, response):
        if response.text != '' :
            rows = response.selector.xpath("//tr")[1:]
            personas = []
            for row in rows:
                columns = row.xpath("./td/text()").extract()
                item = ViaticoPasajeItem()
                item['tipo_poder_id'] = response.meta.get('tipo_poder_id')
                item['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
                item['categoria'] = response.meta.get('categoria')
                item['entidad_id'] = response.meta.get('entidad_id')
                item['entidad_nombre'] = response.meta.get('entidad_nombre')
                item['codmes'] = response.meta.get('codmes')
                item['pk_viaticos'] = columns[0].replace('\n','').strip()
                item['fk_fue_financiamiento'] = columns[0].replace('\n','').strip()
                item['fuente'] = columns[0].replace('\n','').strip()
                item['vc_ruc_entidad'] = columns[0].replace('\n','').strip()
                item['ch_viaticos_tipo'] = columns[0].replace('\n','').strip()
                item['ch_viaticos_ruta'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_anno'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_mes'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_area'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_usuarios'] = columns[0].replace('\n','').strip()
                item['dt_viaticos_fechas'] = columns[0].replace('\n','').strip()
                item['dt_viaticos_fechas_retorno'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_ruta'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_autorizacion'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_costo_pasajes_n'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_via_n'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_total_n'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_costo_pasajes_e'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_via_e'] = columns[0].replace('\n','').strip()
                item['dc_viaticos_total_e'] = columns[0].replace('\n','').strip()
                item['vc_viaticos_resolucion'] = columns[0].replace('\n','').strip()
                yield item
        else:
            self.logger.error(f"No procesó spider: {self.name}, entidad: {response.meta.get('entidad_nombre')}(id: {response.meta.get('entidad_id')}), periodo: {response.meta.get('codmes')}', url: {response.request.url}")
