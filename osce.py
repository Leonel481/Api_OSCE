import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
import json


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
    print(data_f)

# Ejecutar el script asíncrono

if __name__ == '__main__':
    asyncio.run(main())