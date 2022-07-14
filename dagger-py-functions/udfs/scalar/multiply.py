from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.FLOAT())
def multiply(i, j):
    return i * j