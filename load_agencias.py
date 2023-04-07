import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, input_file_name, regexp_extract, concat_ws, udf, regexp_replace, lpad
from pyspark.sql.types import StringType, FloatType, StructType, DoubleType

findspark.init()

spark = SparkSession.builder.master("local").appName("AGENCIAS").getOrCreate()

df = (
    spark.read.csv(
        "agencias_csv",
        inferSchema=False,
        header=True,
        multiLine=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        sep=",",
        quote='"'
    )
)

df = df.withColumn("filename_input", input_file_name())

df = df.withColumn('ano', regexp_extract("filename_input", r'(\d{4})-\d{2}_', 1))
df = df.withColumn('mes', regexp_extract("filename_input", r'\d{4}-(\d{2})_', 1))

df = df.drop("filename_input")
df = df.withColumn("full_endereco", concat_ws(",", "nome_agencia", "endereco", "bairro", "cep", "municipio", "uf"))
df = df.withColumn("cnpj", regexp_replace("cnpj", "\\.|/", ""))
df = df.withColumn("cnpj", lpad("cnpj", 8, "0"))
df = df.withColumn("sequencial_do_cnpj", regexp_replace("sequencial_do_cnpj", "\\.|/", ""))
df = df.withColumn("sequencial_do_cnpj", lpad("sequencial_do_cnpj", 4, "0"))
df = df.withColumn("dv_do_cnpj", regexp_replace("dv_do_cnpj", "\\.|/", ""))
df = df.withColumn("dv_do_cnpj", lpad("dv_do_cnpj", 2, "0"))
df = df.withColumn("cnpj_full", concat_ws("", "cnpj", "sequencial_do_cnpj", "dv_do_cnpj"))
df.write.parquet("agencias_cleared")