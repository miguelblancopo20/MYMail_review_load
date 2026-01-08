from datetime import datetime, timedelta
import os

def calcular_fechas():
    """
    Calcula el lunes de la semana pasada y el lunes anterior.
    Retorna las fechas en formato ISO con timezone UTC.
    """
    hoy = datetime.now()
    
    # Calcular el lunes de esta semana (weekday 0 = lunes)
    dias_desde_lunes = hoy.weekday()
    lunes_esta_semana = hoy - timedelta(days=dias_desde_lunes)
    
    # Lunes de la semana pasada (el "from" - desde)
    lunes_semana_pasada = lunes_esta_semana - timedelta(days=7)
    
    # Lunes de esta semana (el "to" - hasta)
    lunes_anterior = lunes_esta_semana
    
    # Formato ISO a las 00:00 UTC (restar 1 hora para Spain time que es UTC+1)
    # Para representar las 00:00 de España en UTC, usamos las 23:00 del día anterior
    fecha_desde = lunes_semana_pasada.replace(hour=23, minute=0, second=0, microsecond=0) - timedelta(days=1)
    fecha_hasta = lunes_anterior.replace(hour=23, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    return fecha_desde.strftime('%Y-%m-%dT%H:%M:%S.000Z'), fecha_hasta.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def crear_ruta_carpeta():
    """
    Crea la ruta de la carpeta según el mes actual y el lunes anterior.
    Formato: data/mes/dia_mes/
    """
    hoy = datetime.now()
    
    # Calcular el lunes anterior (lunes de esta semana)
    dias_desde_lunes = hoy.weekday()
    lunes_anterior = hoy - timedelta(days=dias_desde_lunes)
    
    # Nombres de meses en español
    meses = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
             'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
    
    mes_nombre = meses[lunes_anterior.month - 1]
    dia_mes = f"{lunes_anterior.day}_{mes_nombre}"
    
    # Crear ruta
    ruta = os.path.join('data', mes_nombre, dia_mes)
    
    # Crear carpeta si no existe
    os.makedirs(ruta, exist_ok=True)
    
    return ruta

# Calcular fechas y crear carpeta
fecha_desde, fecha_hasta = calcular_fechas()
ruta_carpeta = crear_ruta_carpeta()

print(f"Período de consulta:")
print(f"Desde: {fecha_desde}")
print(f"Hasta: {fecha_hasta}")
print(f"Carpeta de destino: {ruta_carpeta}")

# Enlaces actualizados con las fechas calculadas
ia = f"https://bitacora-tcrm.es.telefonica/s/ia-canales/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:5000),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))&_a=(columns:!(idLotus,Location,Sublocation),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'9e5a8de7-50c9-4644-acc6-1f9665b8c135',key:Proyecto,negate:!f,params:(query:MAYORDOMO_MAIL),type:phrase),query:(match_phrase:(Proyecto:MAYORDOMO_MAIL))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Cantidad,negate:!t,params:(query:0),type:phrase),query:(match_phrase:(Cantidad:0))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Type,negate:!t,params:(query:Get_Correos),type:phrase),query:(match_phrase:(Type:Get_Correos))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Type,negate:!f,params:(query:Clasificador),type:phrase),query:(match_phrase:(Type:Clasificador)))),grid:(columns:(Location:(width:292),idLotus:(width:229))),hideChart:!t,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"

rpa = f"https://kibana.es.telefonica/s/rpa-b2b/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:5000),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))&_a=(columns:!(Entorno,Documento,IDU,Origen,Status),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'036a3840-2ddc-11eb-b963-612af0434282',key:Entorno,negate:!f,params:(query:PROD),type:phrase),query:(match_phrase:(Entorno:PROD))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'036a3840-2ddc-11eb-b963-612af0434282',key:Origen,negate:!f,params:!(CATEG-ATOS,CATEG-ML),type:phrases),query:(bool:(minimum_should_match:1,should:!((match_phrase:(Origen:CATEG-ATOS)),(match_phrase:(Origen:CATEG-ML)))))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'036a3840-2ddc-11eb-b963-612af0434282',key:query,negate:!f,type:custom,value:'%7B%22script%22:%7B%22script%22:%7B%22lang%22:%22painless%22,%22source%22:%22doc%5B!'IDU!'%5D.value.length()%20%3C%2020%22%7D%7D%7D'),query:(script:(script:(lang:painless,source:'doc%5B!'IDU!'%5D.value.length()%20%3C%2020'))))),hideChart:!t,index:'036a3840-2ddc-11eb-b963-612af0434282',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"

ia_transacciones = f"https://bitacora-tcrm.es.telefonica/s/ia-canales/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:5000),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))&_a=(columns:!(Sublocation,idLotus,Question,MailToAgent,Location,Subject),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'9e5a8de7-50c9-4644-acc6-1f9665b8c135',key:Proyecto,negate:!f,params:(query:MAYORDOMO_MAIL),type:phrase),query:(match_phrase:(Proyecto:MAYORDOMO_MAIL))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Cantidad,negate:!t,params:(query:0),type:phrase),query:(match_phrase:(Cantidad:0))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Type,negate:!t,params:(query:Get_Correos),type:phrase),query:(match_phrase:(Type:Get_Correos))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',key:Type,negate:!f,params:(query:Clasificador),type:phrase),query:(match_phrase:(Type:Clasificador)))),hideChart:!t,index:'80ac8979-df87-4b7c-966c-bf35e63acda3',interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))"

validaciones = f"https://kibana.es.telefonica/s/rpa-b2b/app/dashboards#/view/880e05f0-7561-11eb-9cc6-ed6e749dad73?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))"

fichas_levantadas = f"https://kibana.es.telefonica/s/rpa-b2b/app/dashboards#/view/880e05f0-7561-11eb-9cc6-ed6e749dad73?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))"

orquestador = f"https://kibana.es.telefonica/s/rpa-b2b/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:5000),time:(from:'{fecha_desde}',to:'{fecha_hasta}'))&_a=(columns:!(Documento,IDU,MatriculaAsesor,Pagename,Queuename),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'036a3840-2ddc-11eb-b963-612af0434282',key:Entorno,negate:!f,params:(query:PROD),type:phrase),query:(match_phrase:(Entorno:PROD)))),index:ca982350-2822-11ec-aaf6-8b2d85b2e61f,interval:auto,query:(language:kuery,query:'Pagename:*Contexto*'),sort:!(!('@timestamp',desc)))"

# Guardar enlaces en la carpeta correspondiente
archivo_salida = os.path.join(ruta_carpeta, 'enlaces_actualizados.txt')
with open(archivo_salida, 'w', encoding='utf-8') as f:
    f.write(f"Enlaces actualizados - Período: {fecha_desde} a {fecha_hasta}\n")
    f.write("="*80 + "\n\n")
    f.write(f"IA:\n{ia}\n\n")
    f.write(f"RPA:\n{rpa}\n\n")
    f.write(f"IA Transacciones:\n{ia_transacciones}\n\n")
    f.write(f"Validaciones:\n{validaciones}\n\n")
    f.write(f"Fichas Levantadas:\n{fichas_levantadas}\n\n")
    f.write(f"Orquestador:\n{orquestador}\n\n")

print(f"\n✓ Enlaces guardados en '{archivo_salida}'")

# Abrir el archivo automáticamente en el bloc de notas
os.startfile(archivo_salida)

