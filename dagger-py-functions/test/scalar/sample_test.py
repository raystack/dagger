from udfs.scalar.sample import sample


def testSample():
    f = sample._func
    assert f("input_text") == "input_text_added_text"
