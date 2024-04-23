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
    start_date = datetime.strptime('2024-04-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-04-10', '%Y-%m-%d')
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

    with open('data_f.json', 'w') as output_file:
        json.dump(data_f, output_file, indent=2)

# Ejecutar el script asíncrono
if __name__ == '__main__':

    # asyncio.run(main())
    asyncio.run(save_data())

    # df = pd.json_normalize(data_f)
    # df['date'] = pd.to_datetime(df['date'], utc=True)
    # df['publishedDate'] = pd.to_datetime(df['publishedDate'], utc=True)
    # df['tender.datePublished'] = pd.to_datetime(df['tender.datePublished'], utc=True)
    # df['tender.tenderPeriod.startDate'] = pd.to_datetime(df['tender.tenderPeriod.startDate'], utc=True)
    # df['tender.tenderPeriod.endDate'] = pd.to_datetime(df['tender.tenderPeriod.endDate'], utc=True)
    # df_reciente = df.groupby('tender.title')['date'].max().reset_index()
    # df_filtrado = pd.merge(df, df_reciente, on=['tender.title', 'date'], how='inner')

    # date_columns = df_filtrado.select_dtypes(include=['datetime64[ns, UTC]']).columns
    # for date_column in date_columns:
    #     df_filtrado[date_column] = df_filtrado[date_column].dt.tz_localize(None)

    # # Ahora puedes exportar tu DataFrame a Excel sin problemas
    # df_filtrado.to_excel('OSCE_TOTAL_2024.xlsx', index=False)



