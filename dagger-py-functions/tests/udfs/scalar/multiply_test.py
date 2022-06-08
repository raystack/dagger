from udfs.scalar.multiply import multiply


def testMultiply():
    value = multiply._func
    assert value(5,10) == 50
