from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def sample(text):
    f = open("data/sample_data.txt", "r")
    data = f.read()
    return text + data
