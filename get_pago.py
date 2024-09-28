import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Diretório do perfil de usuário na raiz
user_data_dir = r"./default"  # Diretório do perfil

# Configurando as opções do Chrome
chrome_options = Options()
chrome_options.add_argument(f"--user-data-dir={user_data_dir}")  # Diretório de dados do perfil
chrome_options.add_argument("--profile-directory=Profile 8")  # Nome do perfil específico
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--disable-gpu")  # Necessário para alguns sistemas

# Inicializando o ChromeDriver com o Service
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Abrir a página
driver.get("https://clarity.microsoft.com/projects/view/o29f0iibyx/dashboard?date=Custom&end=1727405940000&start=1727319540000")

# Esperar 5 segundos antes de prosseguir
time.sleep(5)

# Clicar no botão para abrir o menu de download
download_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Download']"))
)
download_button.click()

# Esperar até que a opção de download do CSV apareça e clicar nela
csv_option = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'sharedComponents_csvDownloadOptionText__cjwrT')]"))
)
csv_option.click()

# Aguardar 1 minuto antes de encerrar
time.sleep(60)

driver.quit()
