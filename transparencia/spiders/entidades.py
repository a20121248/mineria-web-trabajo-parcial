from pathlib import Path

import scrapy
from datetime import datetime
from ..items import EntidadItem, TipoPoderItem

class EntidadesSpider(scrapy.Spider):
    name = "entidades"
    allowed_domains = ['transparencia.gob.pe']
    start_urls = ['https://www.transparencia.gob.pe']

    def parse(self, response):
        tipos = response.selector.xpath("//p[contains(@class, 'list-link')]/a")
        for i,tipo in enumerate(tipos):
            tipo_poder_URL = tipo.xpath('./@href').extract_first()
            lst = tipo_poder_URL.split('=')
            tipo_poder_id = lst[-1] if len(lst) > 1 else -1
            tipo_poder_nombre = tipo.xpath('./text()').extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip()
            meta = {
                'tipo_poder_id': tipo_poder_id,
                'tipo_poder_nombre': tipo_poder_nombre,
            }
            
            self.logger.info(f"Processing TipoPoderItem: {tipo_poder_id}, {tipo_poder_nombre}")
            yield TipoPoderItem(tipo_poder_id=tipo_poder_id, tipo_poder_nombre=tipo_poder_nombre)
            yield scrapy.Request(url=response.urljoin(tipo_poder_URL), callback=self.parse_entidades, meta=meta)

    def parse_entidades(self, response):
        bloques = response.selector.xpath("//div[@class='row bloque-cont']/div/div")
        for bloque in bloques:
            entidades = bloque.xpath("./div[2]/ul/li/a")
            for entidad in entidades:
                item = EntidadItem()
                item['tipo_poder_id'] = response.meta.get('tipo_poder_id')
                item['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
                item['categoria'] = bloque.xpath("./div[1]/h4/text()").extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                item['entidad_id'] = entidad.xpath('./@href').extract_first().split('=')[-1]
                item['entidad_nombre'] = entidad.xpath('./text()').extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                yield item

        self.logger.info("Terminó de procesar las entidades.")
