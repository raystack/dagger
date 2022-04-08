from pyflink.table import ScalarFunction, DataTypes
from pyflink.table.udf import udf

class TestUdf(ScalarFunction):

    def __init__(self):
        self.counter = None

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("udf", "TestUdf").counter("value")

    def eval(self, text: str):
        self.counter.inc()
        return text + "_added_text"

test_udf = udf(TestUdf(), result_type=DataTypes.STRING())