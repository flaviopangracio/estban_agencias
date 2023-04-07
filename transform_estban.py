from zipfile import ZipFile
import os

input_path = "ESTBAN"
output_path = "estban_csv"


def extract_zip(file, path):
    try:
        print(f"Extracting {file}")
        z = ZipFile(file, "r")
        files = z.namelist()
        z.extractall(path)
        z.close()

        for file in files:
            filename = os.path.join(output_path, file)
            with open(filename, "rb") as f:
                content = f.read().decode("latin1").encode("utf-8").decode("utf-8")

            with open(filename, "w") as f:
                f.write(content)

            with open(filename, "r") as f:
                lines = f.readlines()[2:]

            with open(filename, "w") as f:
                f.writelines(lines)

        print(f"{file} extracted!")
    except Exception as e:
        raise e


for file in os.listdir(input_path):
    filepath = os.path.join(input_path, file)
    if filepath.endswith(".zip"):
        extract_zip(file=filepath, path=output_path)
