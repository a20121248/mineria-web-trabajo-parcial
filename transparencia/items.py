import scrapy

class EntidadItem(scrapy.Item):
    tipo_poder_id = scrapy.Field()
    tipo_poder_nombre = scrapy.Field()
    categoria = scrapy.Field()
    entidad_id = scrapy.Field()
    entidad_nombre = scrapy.Field()

class TipoPoderItem(scrapy.Item):
    tipo_poder_id = scrapy.Field()
    tipo_poder_nombre = scrapy.Field()

class PersonaItem(scrapy.Item):
    tipo_poder_id = scrapy.Field()
    tipo_poder_nombre = scrapy.Field()
    categoria = scrapy.Field()
    entidad_id = scrapy.Field()
    entidad_nombre = scrapy.Field()
    codmes = scrapy.Field()
    pk_id_personal = scrapy.Field()
    vc_personal_ruc_entidad = scrapy.Field()
    in_personal_anno = scrapy.Field()
    in_personal_mes = scrapy.Field()
    vc_personal_regimen_laboral = scrapy.Field()
    vc_personal_paterno = scrapy.Field()
    vc_personal_materno = scrapy.Field()
    vc_personal_nombres = scrapy.Field()
    vc_personal_cargo = scrapy.Field()
    vc_personal_dependencia = scrapy.Field()
    mo_personal_remuneraciones = scrapy.Field()
    mo_personal_honorarios = scrapy.Field()
    mo_personal_incentivo = scrapy.Field()
    mo_personal_gratificacion = scrapy.Field()
    mo_personal_otros_beneficios = scrapy.Field()
    mo_personal_total = scrapy.Field()
    vc_personal_observaciones = scrapy.Field()
    fec_reg = scrapy.Field()
    pass