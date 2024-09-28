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
   "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjQ4M0FCMDhFNUYwRDMxNjdEOTRFMTQ3M0FEQTk2RTcyRDkwRUYwRkYiLCJ0eXAiOiJKV1QifQ.eyJqdGkiOiIwOTE4OTgyOS1lMTIxLTQ2NjAtYjZkYi01Zjg1NDY5MWU4ZTAiLCJzdWIiOiIyNDQzODE5MTQwMTU3MjA5Iiwic2NvcGUiOiJEYXRhLkV4cG9ydCIsIm5iZiI6MTcyNzQ3OTQ2OCwiZXhwIjo0ODgxMDc5NDY4LCJpYXQiOjE3Mjc0Nzk0NjgsImlzcyI6ImNsYXJpdHkiLCJhdWQiOiJjbGFyaXR5LmRhdGEtZXhwb3J0ZXIifQ.XXDvVoYkSxXSDWQK9Pi6DFaOPoXtKxioOA3HXxhHa_J8UOgNB6_v2uhWNbLLsVkf4EFmKhfYy_QxRW_XXP49ZgYObEFG8TvYJsM7DOsQNiUoatALrpGdYkZ0iKxgFMtiU3JD6GkG9dv_98EjNM343CFEoRToCa5foLwZaAyeFETVQ2TkFSVgcj9jPf85EC1qINyCMlQRrOqyOpZ3pAddor-20mCJBs4arL43ToLSXqEQxC7297B9hd-D31e6bErFJcPh7-rahiiBMnZ3NLDZXDV8VQIiDy1E3UjIDQ9-_iFPd33rj9BOnJrPkBmBH0Vopmx6uNNZstHk_4aTOgBo8w",
   "Content-type": "application/json"
}

slack_webhook_url = 'https://hooks.slack.com/services/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'

# Função para enviar mensagem para o Slack
def send_slack_message(message):
    payload = {
        'username': 'larrouzinha',  # Nome do bot
        'text': message,
        'icon_emoji': '👧🏼'  # Emoji de menina
    }
    response = requests.post(slack_webhook_url, json=payload)
    if response.status_code != 200:
        print(f"Falha ao enviar mensagem para o Slack: {response.text}")
    else:
        print("Mensagem enviada com sucesso para o Slack!")

# Credenciais do Google BigQuery
credentials_path = "./config/config.json"
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Função para fazer a requisição e processar os dados
def process_request():
    while True:
        # Verifica se são meia-noite e 10
        now = datetime.now()
        if now.hour == 20 and now.minute == 25:
            print("Hora de fazer a requisição!")
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

                # Enviar mensagem ao Slack após o envio dos dados para o BigQuery
                message = (
                    "👋 Oii, desculpa o horário! 😅\n"
                    "Dados do *Clarity* atualizados com sucesso! ✅\n"
                    "tmj mlk! 👍"
                )
                send_slack_message(message)

            elif response.status_code in [500, 403, 401]:
                print("Erro 500 ou 403 ou 401: Servidor retornou erro. Tentando novamente.")
                continue  # Tentar novamente

            else:
                print(f"Erro na requisição: {response.status_code}")
        else:
            # Aguarda 1 minuto antes de checar novamente
            print(f"Aguardando meia-noite e 10. Hora atual: {now.strftime('%H:%M')}")
        time.sleep(60)

# Inicia o loop
process_request()
