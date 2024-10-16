from itemadapter import ItemAdapter
from .items import EntidadItem, TipoPoderItem, PersonaItem, OrdenServicioItem, ViaticoPasajeItem
import os
import pandas as pd
from scrapy.utils.project import get_project_settings

class CsvWriterPipeline(object):
    settings = get_project_settings()

    def process_item(self, item, spider):
        if isinstance(item, TipoPoderItem):
            columns = ['tipo_poder_id', 'tipo_poder_nombre']
            filename = f"{self.settings.get('DATA_DIR')}/1_poderes.txt"
        elif isinstance(item, EntidadItem):
            columns = ['tipo_poder_id', 'tipo_poder_nombre', 'categoria', 'entidad_id', 'entidad_nombre']
            filename = f"{self.settings.get('DATA_DIR')}/2_entidades.txt"
        elif isinstance(item, PersonaItem):
            columns = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','pk_id_personal','vc_personal_ruc_entidad','in_personal_anno','in_personal_mes','vc_personal_regimen_laboral','vc_personal_paterno','vc_personal_materno','vc_personal_nombres','vc_personal_cargo','vc_personal_dependencia','mo_personal_remuneraciones','mo_personal_honorarios','mo_personal_incentivo','mo_personal_gratificacion','mo_personal_otros_beneficios','mo_personal_total','vc_personal_observaciones','fec_reg']
            filename = f"{self.settings.get('DATA_DIR')}/3_personal.txt"
        elif isinstance(item, OrdenServicioItem):
            columns = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','codmes','pk_id_orden','fk_id_orden_tipo','vc_orden_ruc','vc_orden_periodo','vc_orden_numero','vc_orden_numero_siaf','dt_orden_fecha','dc_orden_monto','vc_orden_proveedor','vc_orden_descripcion','dt_orden_fec_reg','in_orden_anno','in_orden_mes']
            filename = f"{self.settings.get('DATA_DIR')}/4_ordenes_de_servicio.txt"
        elif isinstance(item, ViaticoPasajeItem):
            columns = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','codmes','pk_id_orden','fk_id_orden_tipo','vc_orden_ruc','vc_orden_periodo','vc_orden_numero','vc_orden_numero_siaf','dt_orden_fecha','dc_orden_monto','vc_orden_proveedor','vc_orden_descripcion','dt_orden_fec_reg','in_orden_anno','in_orden_mes']
            filename = f"{self.settings.get('DATA_DIR')}/5_viatico_pasaje.txt"
        else:
            return item

        # Create a DataFrame for the single item and write to CSV
        item_df = pd.DataFrame([item], columns=columns)
        
        # Check if the file already exists to determine whether to write headers
        write_header = not os.path.exists(filename)

        # Write the DataFrame to CSV
        item_df.to_csv(filename, sep='\t', header=write_header, index=False, mode='a', encoding="utf-8")

        return item
