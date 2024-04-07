from py4phi.core import encrypt_model, decrypt_model
from pyspark.ml.classification import LogisticRegressionModel, LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

# PLEASE NOTE THAT ENCRYPTION IS PERFORMED INPLACE, I.E. save a copy of your folder/model before encrypting

#  1) Simple folder encryption and decryption without config encryption
# Please create test folder and put some files in for testing purpose beforehand
encrypt_model(
    './test_folder',
    encrypt_config=False
)

decrypt_model(
    './test_folder',
    config_encrypted=False
)

# 2) Create PySpark model and save it; Then encrypt and decrypt it back;
# After that, model is still functioning and can be used as shown below
spark = SparkSession.builder.getOrCreate()
data = [
        {"age": 25, "income": 50000, "is_customer": 1},
        {"age": 30, "income": 75000, "is_customer": 0},
        {"age": 40, "income": 100000, "is_customer": 1},
    ]


df = spark.createDataFrame(data)
assembler = VectorAssembler(inputCols=["age", "income"], outputCol="features")
df_assembled = assembler.transform(df)

lr = LogisticRegression(labelCol="is_customer", featuresCol="features")
model = lr.fit(df_assembled)

model.write().overwrite().save('./model')

encrypt_model('./model', True)

decrypt_model('./model', config_encrypted=True)  # Without this line, model won't be loaded

saved_model = LogisticRegressionModel.load('./model')

df_test = spark.createDataFrame([
    {"age": 35, "income": 60000},
    {"age": 45, "income": 90000},
])
df_test_assembled = assembler.transform(df_test)

predictions = saved_model.transform(df_test_assembled)

predictions.show()
