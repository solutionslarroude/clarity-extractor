import requests
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time

# URL da API
url = 'https://www.clarity.ms/export-data/api/v1/project-live-insights'

# Parâmetros da requisição (usando numOfDays = 1)
params = {"numOfDays": "1"}

# Cabeçalhos da requisição
headers = {
   "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjQ4M0FCMDhFNUYwRDMxNjdEOTRFMTQ3M0FEQTk2RTcyRDkwRUYwRkYiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiJlNWFmOGQ3ZS1lMTE5LTQ4N2YtYTJmMy1lNTAwNTYxZjFhYzUiLCJzdWIiOiIyNDQzODE5MTQwMTU3MjA5Iiwic2NvcGUiOiJEYXRhLkV4cG9ydCIsIm5iZiI6MTcyNzM5NjMxOSwiZXhwIjo0ODgwOTk2MzE5LCJpYXQiOjE3MjczOTYzMTksImlzcyI6ImNsYXJpdHkiLCJhdWQiOiJjbGFyaXR5LmRhdGEtZXhwb3J0ZXIifQ.j3tovq13eckm74SZnx2H4T66YJpYobGWK7uYbcLyya3r2hwi_9kEX1zEeH_seFpb0y42THKYGmRsYVau0LjjlCdLHMsnwtKySlWICHxYeOh9JTwXD9Lry3fg2MCF2qWyxx7lrdiFyyekMK9b2YNa4c0bY7AXhs54FkQWnSeRk6l3U1Wc6gzrEu7ZuKtmOZXlQIaFep8gno3Mer5VGKgvdlw37QXeuT55eQ6OGZUVrb3_FekMnMoEdu7Ordt-xbzxN9JfLaFZdCF2Kj8h5t131HnA4TuNifqVnoRrdfEGdJwzHQH-efwTRdeoaVQDg9VDxmF55_M8mY_Q-xSL2toXvQ",
   "Content-type": "application/json"
}

# Credenciais do Google BigQuery
credentials_path = "./config/config.json"
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Função para fazer a requisição e processar os dados
def process_request():
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # Criando uma lista de registros
        records = []
        for item in data:
            metric_name = item["metricName"]
            for info in item["information"]:
                info["metricName"] = metric_name
                records.append(info)

        # Convertendo a lista de registros para um DataFrame
        df = pd.DataFrame(records)

        # Adicionando a coluna "date" com a data de ontem
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        df['date'] = yesterday

        # Configurações do BigQuery
        project_id = 'larroude-data-prod'
        table_id = 'sales_clarity_ods.ods_clarity_insights'

        # Enviando os dados para o BigQuery
        to_gbq(
            df,
            destination_table=table_id,
            project_id=project_id,
            if_exists='append',
            credentials=credentials
        )

        print("Dados enviados com sucesso para o BigQuery!")
        return True  # Sinalizar sucesso
    elif response.status_code == 500:
        print("Erro 500: Servidor retornou erro, continuando...")
        return False  # Continuar
    else:
        print(f"Erro na requisição: {response.status_code}, parando execução.")
        return True  # Sinalizar que deve parar a execução

# Loop que tenta até receber 200 ou outro status que não seja 500
while True:
    stop = process_request()
    if stop:
        break
    time.sleep(60)  # Aguardar 1 minuto antes de tentar novamente
