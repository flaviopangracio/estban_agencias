import googlemaps
import csv

# substitua a chave API pelo seu próprio
gmaps = googlemaps.Client(key="you-api-key")

# nome do arquivo de entrada e saída
input_file = ""
output_file = ""

# abra o arquivo de entrada e crie um objeto csv.DictReader
with open(input_file, "r") as f:
    reader = csv.DictReader(f)
    # crie um arquivo de saída e escreva o cabeçalho
    with open(output_file, "a", newline="") as f_out:
        writer = csv.DictWriter(
            f_out, fieldnames=["cnpj_full", "endereco", "lat", "lng"]
        )
        # leia cada linha do arquivo de entrada
        for index, row in enumerate(reader):
            if index >= 0:
                # obtenha o endereço da linha atual
                address = row["endereco"]
                cnpj_full = row["cnpj_full"]
                # use a API do Google Maps para obter as coordenadas geográficas
                geocode_result = gmaps.geocode(address)
                # se houver resultados, obtenha a latitude e longitude
                if len(geocode_result) > 0:
                    lat = geocode_result[0]["geometry"]["location"]["lat"]
                    lng = geocode_result[0]["geometry"]["location"]["lng"]
                    # escreva as informações no arquivo de saída
                    writer.writerow(
                        {
                            "cnpj_full": cnpj_full,
                            "endereco": address,
                            "lat": lat,
                            "lng": lng,
                        }
                    )
                else:
                    writer.writerow(
                        {
                            "cnpj_full": cnpj_full,
                            "endereco": address,
                            "lat": None,
                            "lng": None,
                        }
                    )
