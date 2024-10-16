import scrapy
import pandas as pd
from ..items import OrdenServicioItem
from datetime import datetime
from dateutil.relativedelta import relativedelta

class OrdenServicioSpider(scrapy.Spider):
    name = 'ordenes_servicio'
    allowed_domains = ['transparencia.gob.pe']
    personal_URL = 'http://www.transparencia.gob.pe/contrataciones/pte_transparencia_contrataciones_genera.aspx?tipo_seleccion=1&'
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
                yield scrapy.Request(url=file_url, meta=meta, callback=self.parse_orden_servicio)
        
    def parse_orden_servicio(self, response):
        if response.text != '' :
            rows = response.selector.xpath("//tr")[1:]
            personas = []
            for row in rows:
                columns = row.xpath("./td/text()").extract()
                item = OrdenServicioItem()
                item['tipo_poder_id'] = response.meta.get('tipo_poder_id')
                item['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
                item['categoria'] = response.meta.get('categoria')
                item['entidad_id'] = response.meta.get('entidad_id')
                item['entidad_nombre'] = response.meta.get('entidad_nombre')
                item['codmes'] = response.meta.get('codmes')
                item['pk_id_orden'] = columns[0].replace('\n','').strip()
                item['fk_id_orden_tipo'] = columns[1].replace('\n','').strip()
                item['vc_orden_ruc'] = columns[2].replace('\n','').strip()
                item['vc_orden_periodo'] = columns[3].replace('\n','').strip()
                item['vc_orden_numero'] = columns[4].replace('\n','').strip()
                item['vc_orden_numero_siaf'] = columns[5].replace('\n','').strip()
                item['dt_orden_fecha'] = columns[6].replace('\n','').strip()
                item['dc_orden_monto'] = columns[7].replace('\n','').strip()
                item['vc_orden_proveedor'] = columns[8].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                item['vc_orden_descripcion'] = columns[9].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                item['dt_orden_fec_reg'] = columns[10].replace('\n','').strip()
                item['in_orden_anno'] = columns[11].replace('\n','').strip()
                item['in_orden_mes'] = columns[12].replace('\n','').strip()
                
                yield item
        else:
            self.logger.error(f"No procesó spider: {self.name}, entidad: {response.meta.get('entidad_nombre')}(id: {response.meta.get('entidad_id')}), periodo: {response.meta.get('codmes')}', url: {response.request.url}")
