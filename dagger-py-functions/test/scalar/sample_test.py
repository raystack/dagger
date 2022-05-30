from udfs.scalar.sample import sample


def testSample():
    f = sample._func
    assert f("input_text_") == "input_text_sample_text"
