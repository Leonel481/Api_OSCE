import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
import json
import pandas as pd

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()   

async def fetch_all_pages(session, endpoint, start_date, end_date):
    data = []
    query = f'&order=asc&sourceId=seace_v3&startDate={start_date}&endDate={end_date}&mainProcurementCategory=services'
    page_first = 1
    while True:
        url = f'{endpoint}page={page_first}{query}'
        response = await fetch(session, url)
        if not response:
            print(f"No hay más datos en la página {page_first} para el rango de fechas {start_date} a {end_date}. Deteniendo la iteración.")
            break
        data.append(response['releases'])
        page_first += 1
    return data


async def main():
    endpoint = 'https://contratacionesabiertas.osce.gob.pe/api/v1/releases?'
    start_date = datetime.strptime('2024-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-04-23', '%Y-%m-%d')
    tasks = []

    while start_date <= end_date:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = (start_date + timedelta(days=3)).strftime('%Y-%m-%d')
        tasks.append(fetch_all_pages(aiohttp.ClientSession(), endpoint, start_date_str, end_date_str))
        start_date += timedelta(days=3)

    results = await asyncio.gather(*tasks)
    data_f = [item for sublist in results for item in sublist]
    return data_f

async def save_data():
    
    data_f = await main()
    
    data_f = [item for sublist in data_f for item in sublist]
    df = pd.json_normalize(data_f)
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df['publishedDate'] = pd.to_datetime(df['publishedDate'], utc=True)
    df['tender.datePublished'] = pd.to_datetime(df['tender.datePublished'], utc=True)
    df['tender.tenderPeriod.startDate'] = pd.to_datetime(df['tender.tenderPeriod.startDate'], utc=True)
    df['tender.tenderPeriod.endDate'] = pd.to_datetime(df['tender.tenderPeriod.endDate'], utc=True)
    df_reciente = df.groupby('tender.title')['date'].max().reset_index()
    df_filtrado = pd.merge(df, df_reciente, on=['tender.title', 'date'], how='inner')

    date_columns = df_filtrado.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        df_filtrado[date_column] = df_filtrado[date_column].dt.tz_localize(None)

    df_final = df_filtrado[['ocid','date','tag','initiationType','parties','buyer.id','buyer.name','planning.budget.description','tender.id','tender.title','tender.description','tender.procuringEntity.id','tender.datePublished',
                            'tender.procurementMethod','tender.procurementMethodDetails','tender.mainProcurementCategory','tender.value.amount','tender.value.currency','tender.value.amount_PEN','tender.documents','planning.budget.project',
                            'planning.budget.projectID','awards','tender.tenderers','tender.numberOfTenderers']]


    new_name_columns = ['ocid','Fecha','Estado','Tipo_Convocatoria','Partes','Comprador_id','Comprador','Tipo_Comprador','Licitacion_id','Sigla_Nomenclatura','Licitacion_Descripcion','Entidad_Contratante','Fecha_Licitacion_Publicacion',
                            'Metodo_licitacion','Metodo_licitacion_detalle','Categoria_licitacion','Monto','Moneda','Monto_PEN','Documentos','Proyecto','Proyecto_id','Ganador','Licitadores','Cantidad Licitadores']
    
    df_final.columns = new_name_columns
    df_final = df_final.drop_duplicates(subset='Licitacion_id', keep='last')

    filtro = r'backup|almacenamiento|servidores|hosting|housing'

    df_final_filtro = df_final.loc[df_final['Licitacion_Descripcion'].str.contains(filtro, case=False, na=False)]

    # Ahora puedes exportar tu DataFrame a Excel sin problemas
    df_final.to_excel('OSCE_TOTAL_2024.xlsx', index=False)
    df_final_filtro.to_excel('OSCE_TOTAL_2024_FILTRO.xlsx', index=False)

# Ejecutar el script asíncrono
if __name__ == '__main__':

    # asyncio.run(main())
    asyncio.run(save_data())





