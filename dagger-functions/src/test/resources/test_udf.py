from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def test_udf(text: str):
    return text + "_added_text"
