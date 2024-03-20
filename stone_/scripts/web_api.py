
import requests, zipfile, io
from pyspark.sql import SparkSession
from pyspark.sql import types as st
from pyspark.sql import functions as sf

def extract_zip(zip_file_url, path):
    """
    função para extrair zip
    :param zip_file_url:
    :param path:
    :return:
    """
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path)

def read_csv(spark, csv_path, delimiter=',', schema=None):
    """
    função para ler um csv no spark
    :param csv_path:
    :param delimiter:
    :param schema:
    :return:
    """
    df = spark.read. \
        option("header", "false"). \
        option("delimiter", delimiter). \
        schema(schema). \
        csv(csv_path)
    df = df.withColumn("timestamp", sf.current_timestamp())
    return df


def table_empresa_socio(df_empresa, df_socio):
    """
    Cria coluna flg_strangeiro não havia nenhum registro com documento_socio com
    '999' olhei a doc oficial e o tipo_socio 3 é estrangeiro
    Sumariza os dados e gera nova tabela
    :param df_empresa:
    :param df_socio:
    :return: df
    """
    df_socio = df_socio.withColumn("flg_strangeiro",
                                   sf.when(sf.col("tipo_socio") == 3, 1).otherwise(0))

    # Cria nova tabela
    df_final = df_empresa.join(df_socio, ['cnpj'], how='left')
    df_final = df_final.withColumn("flg_strangeiro",
                                   sf.when(sf.col("tipo_socio") == 3, 1).otherwise(0))

    df_final = df_final.groupBy("cnpj").agg(sf.count("*").alias("qtde_socios"),
                                            sf.sum("flg_strangeiro").alias("qtde_socios_estrangeiros")
                                            )
    df_final = df_final.withColumn("timestamp", sf.current_timestamp())

    return df_final


def write_jdbc(df, connection_string, connection_user, connection_pass, table):
    """
    write spark dataframe on jdbc
    :param df:
    :param connection_string:
    :param connection_user:
    :param connection_pass:
    :param table:
    :return:
    """
    df.write.format('jdbc'). \
        option("user", connection_user). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("password", connection_pass). \
        option("url", connection_string). \
        option("dbTable", table). \
        mode("overwrite"). \
        save()
    print(f"{table} criada com sucesso")

def write_parquet(df,path):
    df.write.mode("overwrite").parquet(path)
    print(f"{path} ok")

# create spark context
spark = SparkSession.builder.master("local") \
    .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
    .appName("stone").getOrCreate()

# bronze to save raw data from dadosabertos
bronze_path = 'bronze'

#info conexão AWS
connection_string = "jdbc:mysql://engenharia.cvoiqq4wgtyg.us-east-1.rds.amazonaws.com"
connection_user = "aluno"
connection_pass = "aluno123"

# extract socios1 and empresas1
extract_zip("https://dadosabertos.rfb.gov.br/CNPJ/Socios1.zip", bronze_path)
extract_zip("https://dadosabertos.rfb.gov.br/CNPJ/Empresas1.zip", bronze_path)

# create schema for dataframe  empresa and socio
schema_empresa = st.StructType([
    st.StructField("cnpj", st.StringType(), False),
    st.StructField("razão_social", st.StringType(), False),
    st.StructField("natureza_juridica", st.IntegerType(), False),
    st.StructField("qualificacao_responsavel", st.IntegerType(), False),
    st.StructField("capital_social", st.FloatType(), False),
    st.StructField("cod_porte", st.StringType(), False)])

schema_socio = st.StructType([
    st.StructField("cnpj", st.StringType(), False),
    st.StructField("tipo_socio", st.IntegerType(), False),
    st.StructField("nome_socio", st.StringType(), False),
    st.StructField("documento_socio", st.StringType(), False),
    st.StructField("codigo_qualificacao_socio", st.StringType(), False)
])

df_empresa = read_csv(spark, f"{bronze_path}/K3241.K03200Y1.D40309.EMPRECSV", delimiter=';', schema=schema_empresa)
df_socio = read_csv(spark, f"{bronze_path}/K3241.K03200Y1.D40309.SOCIOCSV", delimiter=';', schema=schema_socio)
df_final = table_empresa_socio(df_empresa, df_socio)

#Grava formato silver
write_parquet(df_empresa,'silver/empresa')
write_parquet(df_socio,'silver/socio')
write_parquet(df_final,'silver/empresa_socio')

#Grava RDS
write_jdbc(df_empresa, connection_string, connection_user, connection_pass, 'ATIVIDADE.empresa')
write_jdbc(df_socio, connection_string, connection_user, connection_pass, 'ATIVIDADE.socio')
write_jdbc(df_final, connection_string, connection_user, connection_pass, 'ATIVIDADE.empresa_socio')

