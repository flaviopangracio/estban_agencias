import findspark
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col
from pyspark.sql.types import StringType, FloatType, StructType

findspark.init()

spark = SparkSession.builder.master("local").appName("ESTBAN").getOrCreate()

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv("estban_csv")
)

df = df.withColumn("ano", substring(col("#DATA_BASE"), 1, 4))
df = df.withColumn("mes", substring(col("#DATA_BASE"), 5, 2))

df = df.drop(col("#DATA_BASE"))

with open("estban.json", "r") as f:
    columns = json.load(f)

for index, column in enumerate(df.columns):
    df = df.withColumnRenamed(column, columns[index]["name"])

def _cast_json(df, columns):

        root_spark_schema = {"fields": [], "type": "struct"}

        bq_schema = columns

        parse = {
            "BIGNUMERIC": "decimal(38, 9)",
            "FLOAT": "double",
            "INTEGER": "long",
            "DATETIME": "timestamp",
            "STRING": "string",
            "TIMESTAMP": "timestamp",
            "DATE": "date",
            "BOOLEAN": "boolean",
            "NUMERIC": "decimal(38, 9)",
        }

        def __bq_to_spark(schema: list, root: dict):
            for column in schema:
                if column["type"] == "RECORD":
                    if column["mode"] == "NULLABLE":
                        if "list" in [
                            field["name"] for field in column["record_fields"]
                        ]:

                            spark_column = {
                                "metadata": {},
                                "name": column["name"],
                                "nullable": False,
                                "type": {
                                    "containsNull": True,
                                    "elementType": {"fields": [], "type": "struct"},
                                    "type": "array",
                                },
                            }

                            root["fields"].append(spark_column)

                            __bq_to_spark(
                                column["record_fields"][0]["record_fields"][0][
                                    "record_fields"
                                ],
                                spark_column["type"]["elementType"],
                            )
                        else:
                            spark_column = {
                                "metadata": {},
                                "name": column["name"],
                                "nullable": column["mode"] == "NULLABLE",
                                "type": "struct",
                                "fields": [],
                            }

                            __bq_to_spark(column["fields"], spark_column)
                    else:
                        spark_column = {
                            "metadata": {},
                            "name": column["name"],
                            "nullable": column["mode"] == "NULLABLE",
                            "type": "array",
                        }

                        root["fields"].append(spark_column)
                else:
                    spark_column = {
                        "metadata": {},
                        "name": column["name"],
                        "nullable": column["mode"] == "NULLABLE",
                        "type": parse[column["type"]],
                    }

                    root["fields"].append(spark_column)

            return root

        spark_schema = __bq_to_spark(schema=bq_schema, root=root_spark_schema)

        spark_schema2 = StructType.fromJson(spark_schema)

        return spark_schema2

schema_parsed = _cast_json(df, columns)

for column in df.schema:
    df = df.withColumn(
        column.name,
        col(column.name).cast(schema_parsed[column.name].dataType)
    )

df.write.parquet("estban_cleared")