from urllib.request import urlretrieve
import requests
from bs4 import BeautifulSoup
import os


host = "https://www.bcb.gov.br"


def get_file(url, filename):
    try:
        urlretrieve(url, filename)
        print(f"{filename} downloaded!")
    except Exception as e:
        print("Finish")
        raise e


# faz uma requisição para a página web
response = requests.get(f"{host}/fis/info/agencias.asp?frame=1")

# cria um objeto BeautifulSoup para analisar o HTML da página
soup = BeautifulSoup(response.text, "html.parser")

# encontra o elemento select na página pelo seu id ou classe
select_element = soup.find("select", {"id": "Agencias"})


# itera sobre todas as opções no elemento select e exibe o valor de cada uma
for option in select_element.find_all("option"):
    filename = f'AGENCIAS/{option["value"].split("/")[-1]}'
    if not os.path.exists(filename):
        get_file(url=f"{host}/{option['value']}", filename=filename)
    else:
        print(f"{filename} already exists!")
