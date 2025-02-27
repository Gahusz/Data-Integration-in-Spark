import requests
from bs4 import BeautifulSoup
from pyspark.sql.functions import col, lit
from pyspark.sql.types import BooleanType, IntegerType

def convert_boolean_columns_to_integer(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name, col(field.name).cast(IntegerType()))
    return df

def fetch_data_from_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

def get_value_from_label(soup, label_text):
    """
    Wyszukuje w HTML parę <td class="notowania-label">label_text</td>
    i zwraca wartość z odpowiadającej <td class="notowania-value">.
    """
    label_td = soup.find('td', class_='notowania-label', text=label_text)
    value_td = label_td.find_next_sibling('td', class_='notowania-value')
    return value_td.text.strip()

def read_table(spark, database_url, table_name, user, password, driver):
    query = f"(SELECT * FROM {table_name}) AS tmp"
    return spark.read \
        .format("jdbc") \
        .option("url", database_url) \
        .option("dbtable", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

def add_piekarnia_id(df, piekarnia_id):
    return df.withColumn("piekarnia_id", lit(piekarnia_id))

def write_mssql_table(df, table_name, mssql_dest_url, mssql_user, mssql_password, mode='overwrite'):
    df = convert_boolean_columns_to_integer(df)
    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", mssql_dest_url) \
        .option("dbtable", table_name) \
        .option("user", mssql_user) \
        .option("password", mssql_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("batchsize", 100000) \
        .mode(mode) \
        .save()

def parse_float_zl(value_str):
    """
    Konwertuje string w formacie np. '1234.56 zł' do float 1234.56
    """
    return float(value_str.replace('zł','').strip())

def parse_float(value_str):
    """
    Konwertuje string z procentami lub innymi znakami na float.
    """
    return float(value_str.strip())
