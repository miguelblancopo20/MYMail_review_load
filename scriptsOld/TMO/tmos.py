import pandas as pd
from datetime import datetime
from funciones_tmos import procesar_df_rpa_procesos, calculate_tmo_contexto, calculate_tmo_normal

mes = "noviembre"

csv_rpa_procesos = 'data/TMOs/' + mes + '/rpa-b2b-procesos.csv'

csv_orquestador_facturacion_contexto = 'data/TMOs/' + mes + '/contexto/tmo_facturas.csv'
csv_orquestador_ayb_contexto = 'data/TMOs/' + mes + '/contexto/tmo_ayb.csv'
csv_orquestador_bajas_contexto = 'data/TMOs/' + mes + '/contexto/tmo_bajas.csv'
csv_orquestador_desvios_contexto = 'data/TMOs/' + mes + '/contexto/tmo_desvios.csv'
csv_orquestador_averiasfijo_contexto = 'data/TMOs/' + mes + '/contexto/tmo_averias_fijo.csv'
csv_orquestador_averiasmovil_contexto = 'data/TMOs/' + mes + '/contexto/tmo_averias_movil.csv'
csv_orquestador_multisim_contexto = 'data/TMOs/' + mes + '/contexto/tmo_multisim.csv'

df_rpa_procesos = procesar_df_rpa_procesos(csv_rpa_procesos=csv_rpa_procesos)

print("####### CONTEXTO #######")

df_duplicado = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_facturacion_contexto, df_procesos=df_rpa_procesos, tematica='Duplicado de Factura')
df_ayb = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_ayb_contexto, df_procesos=df_rpa_procesos, tematica='Altas y Bajas')
df_bajalinea = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_bajas_contexto, df_procesos=df_rpa_procesos, tematica='Baja de Línea')
df_averiasfijo = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_averiasfijo_contexto, df_procesos=df_rpa_procesos, tematica='Averías Fijo')
df_averiasmovil = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_averiasmovil_contexto, df_procesos=df_rpa_procesos, tematica='Averías Móvil')
df_desvios = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_desvios_contexto, df_procesos=df_rpa_procesos, tematica='Desvíos')
df_multisim = calculate_tmo_contexto(csv_rpa_orquestador=csv_orquestador_multisim_contexto, df_procesos=df_rpa_procesos, tematica='Multisim')

df_duplicado.to_excel('data/TMOs/' + mes + '/TMO_duplicado_contexto.xlsx', index=False)
df_ayb.to_excel('data/TMOs/' + mes + '/TMO_ayb_contexto.xlsx', index=False)
df_bajalinea.to_excel('data/TMOs/' + mes + '/TMO_bajalinea_contexto.xlsx', index=False)
df_averiasfijo.to_excel('data/TMOs/' + mes + '/TMO_averiasfijo_contexto.xlsx', index=False)
df_averiasmovil.to_excel('data/TMOs/' + mes + '/TMO_averiasmovil_contexto.xlsx', index=False)
df_desvios.to_excel('data/TMOs/' + mes + '/TMO_desvios_contexto.xlsx', index=False)
df_multisim.to_excel('data/TMOs/' + mes + '/TMO_multisim_contexto.xlsx', index=False)


csv_cierre_contacto = 'data/TMOs/' + mes + '/todo/cierre_contacto.csv'

csv_orquestador_facturacion_normal = 'data/TMOs/' + mes + '/todo/tmo_facturas.csv'
csv_orquestador_ayb_normal = 'data/TMOs/' + mes + '/todo/tmo_ayb.csv'
csv_orquestador_bajas_normal = 'data/TMOs/' + mes + '/todo/tmo_bajas.csv'
csv_orquestador_desvios_normal = 'data/TMOs/' + mes + '/todo/tmo_desvios.csv'
csv_orquestador_averiasfijo_normal = 'data/TMOs/' + mes + '/todo/tmo_averias_fijo.csv'
csv_orquestador_averiasmovil_normal = 'data/TMOs/' + mes + '/todo/tmo_averias_movil.csv'
csv_orquestador_multisim_normal = 'data/TMOs/' + mes + '/todo/tmo_multisim.csv'

print("####### NORMAL #######")

df_duplicado = calculate_tmo_normal(csv_orquestador_facturacion_normal, df_rpa_procesos, csv_cierre_contacto, 'Duplicado de Factura')
df_duplicado = calculate_tmo_normal(csv_orquestador_ayb_normal, df_rpa_procesos, csv_cierre_contacto, 'Altas y Bajas')
df_duplicado = calculate_tmo_normal(csv_orquestador_bajas_normal, df_rpa_procesos, csv_cierre_contacto, 'Baja de Línea')
df_duplicado = calculate_tmo_normal(csv_orquestador_desvios_normal, df_rpa_procesos, csv_cierre_contacto, 'Averías Fijo')
df_duplicado = calculate_tmo_normal(csv_orquestador_averiasfijo_normal, df_rpa_procesos, csv_cierre_contacto, 'Averías Móvil')
df_duplicado = calculate_tmo_normal(csv_orquestador_averiasmovil_normal, df_rpa_procesos, csv_cierre_contacto, 'Desvíos')
df_duplicado = calculate_tmo_normal(csv_orquestador_multisim_normal, df_rpa_procesos, csv_cierre_contacto, 'Multisim')
