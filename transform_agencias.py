from zipfile import ZipFile
import os

input_path = "AGENCIAS"
output_path = "agencias_xls"


def extract_zip(file, path):
    try:
        print(f"Extracting {file}")
        z = ZipFile(file, "r")
        z.extractall(path)
        z.close()

        print(f"{file} extracted!")
    except Exception as e:
        raise e


for file in os.listdir(input_path):
    filepath = os.path.join(input_path, file)
    if filepath.endswith(".zip"):
        extract_zip(file=filepath, path=output_path)
