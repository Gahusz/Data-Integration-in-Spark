import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr
from pyspark.sql.types import DecimalType

from functions import (
    convert_boolean_columns_to_integer,
    fetch_data_from_page,
    get_value_from_label,
    read_table,
    add_piekarnia_id,
    write_mssql_table,
    parse_float_zl,
    parse_float
)

# ======================================
# Konfiguracja środowiska
# ======================================

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-11'
os.environ['SPARK_HOME'] = r'C:\Users\User\Desktop\spark\spark-3.1.3-bin-hadoop3.2'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['SPARK_HOME'], 'bin')
os.environ['PYSPARK_PYTHON'] = r'C:\Users\User\anaconda3\envs\spark_env\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\User\anaconda3\envs\spark_env\python.exe'

# ======================================
# Parametry połączeń
# ======================================
mssql_source_url = "jdbc:sqlserver://DESKTOP-EOVFFAV:1433;databaseName=Piekarnia2"
mssql_user = "admin"
mssql_password = "12345qwerty"

mysql_url = "jdbc:mysql://localhost:3306/piekarnia?useSSL=false&allowPublicKeyRetrieval=true"
mysql_user = "root"
mysql_password = "admin"

mssql_dest_url = "jdbc:sqlserver://DESKTOP-EOVFFAV:1433;databaseName=Piekarnia_ETL"

# ======================================
# Tworzenie sesji Spark
# ======================================

spark = SparkSession.builder \
    .appName("ETL_Process_for_Piekarnia_ETL") \
    .master("local[6]") \
    .config("spark.jars.packages", 
            "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0,"
            "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8,"
            "mysql:mysql-connector-java:8.0.26") \
    .config("spark.driver.memory", "9g") \
    .config("spark.executor.memory", "9g") \
    .config("spark.executor.cores", "8") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.default.parallelism", "24") \
    .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", "2000")

print(spark)

# ======================================
# Etap Extract
# ======================================
print("Etap Extract...")
print("Pobieranie danych dla pszenicy...")
pszenica_url = "https://www.agrofakt.pl/notowania/29-pszenica-konsumpcyjna/"
soup_pszenica = fetch_data_from_page(pszenica_url)

pszenica_aktualna_cena_str = get_value_from_label(soup_pszenica, "Aktualna cena pszenicy konsumpcyjnej:")
pszenica_aktualna_cena_ton = parse_float_zl(pszenica_aktualna_cena_str)
pszenica_aktualna_cena_kg = pszenica_aktualna_cena_ton / 1000

pszenica_ostatnia_zmiana_str = get_value_from_label(soup_pszenica, "Ostatnia zmiana ceny [%]:")
pszenica_ostatnia_zmiana_procent = parse_float(pszenica_ostatnia_zmiana_str)

pszenica_poczatek_okresu = get_value_from_label(soup_pszenica, "Początek okresu:")
pszenica_koniec_okresu = get_value_from_label(soup_pszenica, "Koniec okresu:")

pszenica_zmiana_od_poczatku_str = get_value_from_label(soup_pszenica, "Zmiana ceny od początku okresu:")
pszenica_zmiana_od_poczatku = parse_float(pszenica_zmiana_od_poczatku_str)

pszenica_srednia_cena_str = get_value_from_label(soup_pszenica, "Średnia cena pszenica konsumpcyjna:")
pszenica_srednia_cena = parse_float_zl(pszenica_srednia_cena_str)

print(f"Aktualna cena pszenicy: {pszenica_aktualna_cena_ton} zł/ton, {pszenica_aktualna_cena_kg} zł/kg")

# ======================================
# Pobieranie danych dla żyta
# ======================================
print("Pobieranie danych dla żyta...")
zyto_url = "https://www.agrofakt.pl/notowania/16-zyto-konsumpcyjne/"
soup_zyto = fetch_data_from_page(zyto_url)

zyto_aktualna_cena_str = get_value_from_label(soup_zyto, "Aktualna cena Żyto konsumpcyjne:")
zyto_aktualna_cena_ton = parse_float_zl(zyto_aktualna_cena_str)
zyto_aktualna_cena_kg = zyto_aktualna_cena_ton / 1000

zyto_ostatnia_zmiana_str = get_value_from_label(soup_zyto, "Ostatnia zmiana ceny [%]:")
zyto_ostatnia_zmiana_procent = parse_float(zyto_ostatnia_zmiana_str)

zyto_poczatek_okresu = get_value_from_label(soup_zyto, "Początek okresu:")
zyto_koniec_okresu = get_value_from_label(soup_zyto, "Koniec okresu:")

zyto_zmiana_od_poczatku_str = get_value_from_label(soup_zyto, "Zmiana ceny od początku okresu:")
zyto_zmiana_od_poczatku = parse_float(zyto_zmiana_od_poczatku_str)

zyto_srednia_cena_str = get_value_from_label(soup_zyto, "Średnia cena Żyto konsumpcyjne:")
zyto_srednia_cena = parse_float_zl(zyto_srednia_cena_str)

# ======================================
# Pobieranie danych z baz danych...
# ======================================
print("Pobieranie danych z baz danych...")
df_piekarnia2_dokumenty = read_table(spark, mssql_source_url, "Dokumenty", mssql_user, mssql_password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
df_piekarnia1_dokumenty = read_table(spark, mysql_url, "Dokumenty", mysql_user, mysql_password, "com.mysql.cj.jdbc.Driver")

df_piekarnia2_pozycje = read_table(spark, mssql_source_url, "DokumentyPozycje", mssql_user, mssql_password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
df_piekarnia1_pozycje = read_table(spark, mysql_url, "DokumentyPozycje", mysql_user, mysql_password, "com.mysql.cj.jdbc.Driver")

df_piekarnia2_oferta = read_table(spark, mssql_source_url, "Oferta", mssql_user, mssql_password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
df_piekarnia1_oferta = read_table(spark, mysql_url, "Oferta", mysql_user, mysql_password, "com.mysql.cj.jdbc.Driver")

df_piekarnia2_receptury = read_table(spark, mssql_source_url, "Receptury", mssql_user, mssql_password, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
df_piekarnia1_receptury = read_table(spark, mysql_url, "Receptury", mysql_user, mysql_password, "com.mysql.cj.jdbc.Driver")

# ======================================
# Etap Transform
# ======================================
print("Etap transform...")
print("Konwersja kolumn typu boolean...")
df_piekarnia1_dokumenty = convert_boolean_columns_to_integer(df_piekarnia1_dokumenty)
df_piekarnia2_dokumenty = convert_boolean_columns_to_integer(df_piekarnia2_dokumenty)
df_piekarnia1_pozycje = convert_boolean_columns_to_integer(df_piekarnia1_pozycje)
df_piekarnia2_pozycje = convert_boolean_columns_to_integer(df_piekarnia2_pozycje)
df_piekarnia1_oferta = convert_boolean_columns_to_integer(df_piekarnia1_oferta)
df_piekarnia2_oferta = convert_boolean_columns_to_integer(df_piekarnia2_oferta)


print("Dodawanie ID piekarni...")
df_piekarnia2_dokumenty = add_piekarnia_id(df_piekarnia2_dokumenty, 2)
df_piekarnia1_dokumenty = add_piekarnia_id(df_piekarnia1_dokumenty, 1)

df_piekarnia2_pozycje = add_piekarnia_id(df_piekarnia2_pozycje, 2)
df_piekarnia1_pozycje = add_piekarnia_id(df_piekarnia1_pozycje, 1)

df_piekarnia2_oferta = add_piekarnia_id(df_piekarnia2_oferta, 2)
df_piekarnia1_oferta = add_piekarnia_id(df_piekarnia1_oferta, 1)

df_piekarnia2_receptury = add_piekarnia_id(df_piekarnia2_receptury, 2)
df_piekarnia1_receptury = add_piekarnia_id(df_piekarnia1_receptury, 1)

print("Łączenie tabel...")
df_dokumenty = df_piekarnia1_dokumenty.unionByName(df_piekarnia2_dokumenty, allowMissingColumns=True)
df_pozycje = df_piekarnia1_pozycje.unionByName(df_piekarnia2_pozycje, allowMissingColumns=True)
df_oferta = df_piekarnia1_oferta.unionByName(df_piekarnia2_oferta, allowMissingColumns=True)
df_receptury = df_piekarnia1_receptury.unionByName(df_piekarnia2_receptury, allowMissingColumns=True)

print("Transformacje tabel...")
df_dokumenty = df_dokumenty.withColumn("rok", expr("YEAR(TO_TIMESTAMP(dok_data_wyst, 'yyyy-MM-dd HH:mm:ss.SSS'))")) \
                           .withColumn("miesiac", expr("MONTH(TO_TIMESTAMP(dok_data_wyst, 'yyyy-MM-dd HH:mm:ss.SSS'))")) \
                           .withColumn("status", when(col("dok_wartosc_netto") > 15000, "wysoki")
                                                 .when(col("dok_wartosc_netto") > 7000, "średni")
                                                 .otherwise("niski"))

df_pozycje = df_pozycje.withColumn("dkp_wartosc_pozycji", (col("dkp_ilosc") * col("dkp_cena_kraj")).cast("decimal(12,2)"))
df_oferta = df_oferta.withColumn("ofr_marza", (col("ofr_cena_przychodu") - col("ofr_koszt_produkcji")).cast("decimal(12,2)"))


# ======================================
# Dane o cenach w tabeli Ceny
# ======================================
ceny_schema = ["rodzaj_zboza", "aktualna_cena_ton", "aktualna_cena_kg", "ostatnia_zmiana_procent", "poczatek_okresu", "koniec_okresu", "zmiana_ceny_od_poczatku_okresu", "srednia_cena"]
ceny_data = [
    (
        "pszenica",
        pszenica_aktualna_cena_ton,
        pszenica_aktualna_cena_kg,
        pszenica_ostatnia_zmiana_procent,
        pszenica_poczatek_okresu,
        pszenica_koniec_okresu,
        pszenica_zmiana_od_poczatku,
        pszenica_srednia_cena
    ),
    (
        "zyto",
        zyto_aktualna_cena_ton,
        zyto_aktualna_cena_kg,
        zyto_ostatnia_zmiana_procent,
        zyto_poczatek_okresu,
        zyto_koniec_okresu,
        zyto_zmiana_od_poczatku,
        zyto_srednia_cena
    )
]

df_ceny = spark.createDataFrame(ceny_data, ceny_schema) \
    .withColumn("aktualna_cena_ton", col("aktualna_cena_ton").cast(DecimalType(12,2))) \
    .withColumn("aktualna_cena_kg", col("aktualna_cena_kg").cast(DecimalType(12,4))) \
    .withColumn("ostatnia_zmiana_procent", col("ostatnia_zmiana_procent").cast(DecimalType(5,2))) \
    .withColumn("zmiana_ceny_od_poczatku_okresu", col("zmiana_ceny_od_poczatku_okresu").cast(DecimalType(12,2))) \
    .withColumn("srednia_cena", col("srednia_cena").cast(DecimalType(12,2)))


# ======================================
# Etap Load
# ======================================
write_mssql_table(df_dokumenty, "Dokumenty", mssql_dest_url, mssql_user, mssql_password)
write_mssql_table(df_pozycje, "DokumentyPozycje", mssql_dest_url, mssql_user, mssql_password)
write_mssql_table(df_oferta, "Oferta", mssql_dest_url, mssql_user, mssql_password)
write_mssql_table(df_receptury, "Receptury", mssql_dest_url, mssql_user, mssql_password)
write_mssql_table(df_ceny, "Ceny", mssql_dest_url, mssql_user, mssql_password, mode='append')

print("ETL zakończony pomyślnie.")
spark.stop()
