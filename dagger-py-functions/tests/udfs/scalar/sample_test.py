from udfs.scalar.sample import sample


def testSample():
    value = sample._func
    assert value("input_text_") == "input_text_sample_text"
