from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def sample(text):
    try:
        file = open("data.zip/data/sample_data.txt", "r")
    except FileNotFoundError:
        file = open("data/sample_data.txt", "r")
    data = file.read()
    return text + data
