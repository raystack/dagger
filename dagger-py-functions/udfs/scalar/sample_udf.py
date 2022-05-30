from pyflink.table import ScalarFunction, DataTypes
from pyflink.table.udf import udf

class SampleUdf(ScalarFunction):

    def eval(self, text: str):
        return text + "_added_text"

sample_udf = udf(SampleUdf(), result_type=DataTypes.STRING())