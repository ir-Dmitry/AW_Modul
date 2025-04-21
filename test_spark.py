from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
    collect_list,
    struct,
)
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import udf
from decimal import Decimal


def replace_comma(s):
    try:
        return Decimal(s.replace(",", "."))
    except:
        return None


# Создание SparkSession
spark = SparkSession.builder.appName("Group by Form and Create Hierarchy").getOrCreate()

# Пример данных
data = [
    {
        "nom": "1",
        "name": "Argentina",
        "year": 2005,
        "form": "Republic",
        "balance": 4.699,
    },
    {
        "nom": "2",
        "name": "Brazil",
        "year": 2005,
        "form": "Federation",
        "balance": 15.432,
    },
    {"nom": "1", "name": "Chile", "year": 2007, "form": "Republic", "balance": 9.876},
    {"nom": "1", "name": "Chile", "year": 2007, "form": "Republic", "balance": 23.876},
    {
        "nom": "1",
        "name": "Paraguay",
        "year": 2008,
        "form": "Federation",
        "balance": 3.214,
    },
]
# with open("data/merged_output.json", "r", encoding="utf-8") as file:
#     data = json.load(file)

schema = StructType(
    [
        StructField("jsql__id", IntegerType(), True),
        # StructField("jsql__data", StringType(), True),
        # StructField("jsql__usd", StringType(), True),
        # StructField("jsql__cny", StringType(), True),
        # StructField("jsql__eur", StringType(), True),
        # StructField("jsql__byn", StringType(), True),
        StructField("calc__jsql__data_year", IntegerType(), True),
        StructField("calc__jsql__data_day", IntegerType(), True),
        StructField("calc__jsql__data_month_as_string", StringType(), True),
        StructField("calc__jsql__data_month", IntegerType(), True),
        StructField("calc__jsql__data_quarter", IntegerType(), True),
    ]
)

# df = spark.createDataFrame(data=data, schema=schema)
# df = df.withColumn("jsql__usd", replace_comma(df["jsql__usd"]))
# df = df.withColumn("jsql__cny", replace_comma(df["jsql__cny"]))


print(data)

df = spark.createDataFrame(data)


# Создание DataFrame

# Группировка по "form", суммирование "balance" и создание иерархии
# form_group = df.groupBy(["year", "form", "name"]).agg(
#     spark_sum("balance").alias("balance"),  # Суммирование balance
#     collect_list(struct(col("balance"))).alias(
#         "child_0"
#     ),  # Список объектов name и balance
# )
# # Группировка по "year" с вложенными данными по "form"
# year_group = form_group.groupBy("year", "form").agg(
#     spark_sum("balance").alias("balance"),  # Суммирование balance
#     collect_list(struct(col("name"), col("balance"), col("child_0"))).alias(
#         "child_1"
#     ),  # Вложенные данные по form
# )

# Группировка по "year" с вложенными данными по "form"
# column_list = ["name", "balance"]
# grouped = df.groupBy(["form"]).agg(
#     spark_sum("balance").alias("balance"),  # Суммирование balance
#     collect_list(struct(*[col(c) for c in column_list])).alias(
#         "child"
#     ),  # Вложенные данные по form
# )


def recursive_sum(df, levels, value_column=["balance"], i=0):
    # Получаем последний элемент из levels
    last_element = [levels[-i]] if i else []

    group_columns = last_element + value_column
    if i > 1:
        group_columns.append(f"child_{i-1}")

    # Определяем уровни группировки
    next_levels = levels[:-i] if i else levels[:]

    # Группировка данных
    aggregation = [
        spark_sum(col(v)).alias(v)
        for v in value_column  # Суммирование по каждому value_column
    ]
    if i:
        aggregation.append(
            collect_list(struct(*[col(c) for c in group_columns])).alias(f"child_{i}")
        )

    grouped = df.groupBy(next_levels).agg(*aggregation)

    # Рекурсия для следующего уровня
    if len(levels) - i > 1:
        return recursive_sum(grouped, levels, value_column, i + 1)

    return grouped


def recursive_count(df, levels, value_column=["balance"], original_df=None, i=0):
    """
    Рекурсивная функция для подсчета количества значений относительно первичных данных.

    :param df: Текущий DataFrame на уровне группировки
    :param levels: Список уровней группировки
    :param value_column: Список колонок для подсчета
    :param original_df: Исходный DataFrame (используется для корректного подсчета)
    :param i: Текущий уровень рекурсии
    :return: Группированный DataFrame
    """
    if original_df is None:
        original_df = df

    # Получаем текущий уровень группировки
    last_element = [levels[-i]] if i else []

    group_columns = last_element + value_column
    if i > 1:
        group_columns.append(f"child_{i-1}")

    # Определяем уровни группировки
    next_levels = levels[:-i] if i else levels[:]

    print("last_element = ", last_element)
    print(f"  next_levels_{i} = ", next_levels)
    print(f"  column_list_{i} = ", group_columns)

    # Подсчет общего количества записей для текущей группировки
    count_aggregations = [
        spark_count(original_df[v]).alias(f"count_{v}") for v in value_column
    ]

    # Агрегаты для вложенных данных
    child_aggregation = (
        collect_list(struct(*[col(c) for c in group_columns])).alias(f"child_{i}")
        if i
        else None
    )

    # Формирование списка агрегатов
    aggregation = count_aggregations
    if child_aggregation is not None:
        aggregation.append(child_aggregation)

    # Группировка данных
    grouped = df.groupBy(next_levels).agg(*aggregation)

    # Рекурсия для следующего уровня
    if len(levels) - i > 1:
        return recursive_count(grouped, levels, value_column, original_df, i + 1)

    return grouped


levels = [
    "nom",
    "form",
    "name",
]

value_column = ["year", "balance"]
# Используем рекурсивную функцию
# hierarchical_data = recursive_sum(df, levels, value_column)
hierarchical_data = recursive_count(df, levels, value_column)


# result = year_group
result = hierarchical_data

# Преобразование результата в формат JSON
result_json = result.toJSON().collect()
# Получение JSON-строк для каждой группы

# Конвертация в иерархический формат
hierarchical_data = [json.loads(row) for row in result_json]

# Запись в JSON-файл
with open("result.json", "w", encoding="utf-8") as json_file:
    json.dump(hierarchical_data, json_file, indent=4, ensure_ascii=False)

# Вывод для проверки
print(json.dumps(hierarchical_data, indent=4, ensure_ascii=False))

# Завершение работы
spark.stop()
