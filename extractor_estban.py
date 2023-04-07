from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.request import urlretrieve
import os


host = "https://www4.bcb.gov.br/"

INITIAL_DATE = datetime(1988, 7, 1)
FINAL_DATE = datetime.now()

date = INITIAL_DATE


def get_file(url, filename):
    try:
        urlretrieve(url, filename)
        print(f"{filename} downloaded!")
    except Exception as e:
        print("Finish")
        raise e


while date <= FINAL_DATE:
    url = f'fis/cosif/cont/estban/agencia/{date.strftime("%Y%m")}_ESTBAN_AG.ZIP'
    filename = f'ESTBAN/estban_{date.strftime("%Y-%m")}.zip'

    if not os.path.exists(filename):
        get_file(url=f"{host}/{url}", filename=filename)
    else:
        print(f"{filename} already exists!")
    date += relativedelta(months=1)
