import scrapy
import pandas as pd
from ..items import PersonaItem
from datetime import datetime
from dateutil.relativedelta import relativedelta

class PersonalSpider(scrapy.Spider):
    name = 'personal'
    allowed_domains = ['transparencia.gob.pe']
    personal_URL = 'http://www.transparencia.gob.pe/personal/pte_transparencia_personal_genera.aspx?ch_tipo_regimen=0&vc_dni_funcionario=&vc_nombre_funcionario=&ch_tipo_descarga=1&'
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

                self.logger.info(f"Procesando Personal para {entidad['entidad_nombre']} (entidad_id={entidad['entidad_id']}) en periodo {codmes}'.")
                file_url = f"{self.personal_URL}id_entidad={entidad['entidad_id']}&in_anno_consulta={anho}&ch_mes_consulta={mes}"
                yield scrapy.Request(url=file_url, meta=meta, callback=self.parse_personal)
        
    def parse_personal(self, response):
        url = response.request.url
        if response.text != '' :
            rows = response.selector.xpath("//tr")[1:]
            personas = []
            for row in rows:
                columns = row.xpath("./td/text()").extract()
                persona = PersonaItem()
                persona['tipo_poder_id'] = response.meta.get('tipo_poder_id')
                persona['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
                persona['categoria'] = response.meta.get('categoria')
                persona['entidad_id'] = response.meta.get('entidad_id')
                persona['entidad_nombre'] = response.meta.get('entidad_nombre')
                persona['codmes'] = response.meta.get('codmes')
                persona['pk_id_personal'] = columns[0].replace('\n','').strip()
                persona['vc_personal_ruc_entidad'] = columns[1].replace('\n','').strip()
                persona['in_personal_anno'] = columns[2].replace('\n','').strip()
                persona['in_personal_mes'] = columns[3].replace('\n','').strip()
                persona['vc_personal_regimen_laboral'] = columns[4].replace('\n','').strip()
                persona['vc_personal_paterno'] = columns[5].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_materno'] = columns[6].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_nombres'] = columns[7].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_cargo'] = columns[8].replace('\n','').replace('\r',' ').replace('\t',' ').replace('\r',' ').replace('\t',' ').strip()
                persona['vc_personal_dependencia'] = columns[9].replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                persona['mo_personal_remuneraciones'] = columns[10].replace('\n','').strip()
                persona['mo_personal_honorarios'] = columns[11].replace('\n','').strip()
                persona['mo_personal_incentivo'] = columns[12].replace('\n','').strip()
                persona['mo_personal_gratificacion'] = columns[13].replace('\n','').strip()
                persona['mo_personal_otros_beneficios'] = columns[14].replace('\n','').strip()
                persona['mo_personal_total'] = columns[15].replace('\n','').strip()
                persona['vc_personal_observaciones'] = columns[16].replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                persona['fec_reg'] = columns[17].replace('\n','').strip()
                
                yield persona
        else:
            self.logger.info(f"No procesó url='{url}'.")