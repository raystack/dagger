from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def sample(text):
    file = open("data/sample_data.txt", "r")
    data = file.read()
    return text + data
