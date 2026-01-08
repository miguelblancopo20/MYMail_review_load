import pandas as pd
from datetime import datetime

lista_no_asesores = ['t732901', 't736366', 't741302', 't741304'] #Excluímos matrículas que no corresponden a asesores reales
    
def parse_dates(inicio_duplicado, apertura_ficha):
    date_format = "%b %d, %Y @ %H:%M:%S.%f"
    inicio_duplicado = datetime.strptime(inicio_duplicado, date_format)
    apertura_ficha = datetime.strptime(apertura_ficha, date_format)
    return (inicio_duplicado-apertura_ficha).total_seconds()

def procesar_df_rpa_procesos(csv_rpa_procesos):
    df_procesos = pd.read_csv(csv_rpa_procesos)
    df_procesos['IDU'] = df_procesos['IDU'].apply(lambda x: str(x).split('-')[0])

    df_procesos['MatriculaAsesor'] = df_procesos['MatriculaAsesor'].apply(lambda x: x.split('@')[0])
    df_procesos = df_procesos[~df_procesos['MatriculaAsesor'].isin(lista_no_asesores)]
    df_procesos = df_procesos.drop_duplicates(subset=['IDU'], keep='first')
    df_procesos = df_procesos[['IDU', '@timestamp']]

    df_procesos = df_procesos.rename(columns={'@timestamp': 'hora_apertura_ficha'})

    return df_procesos

def calculate_tmo_contexto(csv_rpa_orquestador, df_procesos, tematica):
    df_tematica_contexto = pd.read_csv(csv_rpa_orquestador)
    df_tematica_contexto['IDU'] = df_tematica_contexto['IDU'].apply(lambda x: str(x).split('-')[0])


    df_tematica_contexto['MatriculaAsesor'] = df_tematica_contexto['MatriculaAsesor'].apply(lambda x: x.split('@')[0])
    df_tematica_contexto = df_tematica_contexto[~df_tematica_contexto['MatriculaAsesor'].isin(lista_no_asesores)]
    df_tematica_contexto = df_tematica_contexto[['IDU', '@timestamp']]


    df_tematica_contexto = df_tematica_contexto.rename(columns={'@timestamp': 'tiempo_inicio'})
    df_tematica_contexto = df_tematica_contexto[['IDU', 'tiempo_inicio']]

    df = pd.merge(df_tematica_contexto, df_procesos, how='inner', on='IDU')

    df['Tiempo Total(s)'] = df.apply(lambda x: parse_dates(x.tiempo_inicio, x.hora_apertura_ficha), axis =1)
    df = df[df['Tiempo Total(s)'] >= 0]
    #df = df[df['Tiempo Total(s)'] <= 1000]

    if tematica == 'Duplicado de Factura':
        mediana_tiempos = df['Tiempo Total(s)'].median()
        print(f"Mediana de tiempos en {tematica}: {mediana_tiempos} s")
    else:
        media_tiempos = df['Tiempo Total(s)'].mean()
        print(f"Media de tiempos en {tematica}: {media_tiempos} s")
    return df
    
def calculate_tmo_normal(csv_rpa_orquestador, df_procesos, csv_cierre_contacto, tematica):
    df_orquestador = pd.read_csv(csv_rpa_orquestador)
    df_orquestador['IDU'] = df_orquestador['IDU'].apply(lambda x: str(x).split('-')[0])

    df_orquestador['MatriculaAsesor'] = df_orquestador['MatriculaAsesor'].apply(lambda x: x.split('@')[0])
    df_orquestador = df_orquestador[~df_orquestador['MatriculaAsesor'].isin(lista_no_asesores)]
    df_orquestador = df_orquestador[['IDU']]

    df_cierre_contacto = pd.read_csv(csv_cierre_contacto)
    df_cierre_contacto['IDU'] = df_cierre_contacto['IDU'].apply(lambda x: str(x).split('-')[0])
    df_cierre_contacto = df_cierre_contacto.rename(columns={'@timestamp': 'timestamp'})

    df = pd.merge(df_cierre_contacto, df_orquestador, on='IDU')
    df=df.drop_duplicates(subset=['IDU'], keep='first')

    df = pd.merge(df, df_procesos, on='IDU')

    df['Tiempo Total(s)'] = df.apply(lambda x: parse_dates(x.timestamp, x.hora_apertura_ficha), axis =1)
    df = df[df['Tiempo Total(s)'] >= 0]

    if tematica:
        mediana_tiempos = df['Tiempo Total(s)'].median()
        print(f"Mediana de tiempos en {tematica}: {mediana_tiempos} s")
    else:
        media_tiempos = df['Tiempo Total(s)'].mean()
        print(f"Media de tiempos en {tematica}: {media_tiempos} s")
    return df

