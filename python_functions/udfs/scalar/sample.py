from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.FLOAT())
def sample(text):
    return text + "_added_text"
