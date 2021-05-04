from generator.views.lookml_utils import escape_filter_expr


def test_escape_char():
    expr = "a_b"
    assert escape_filter_expr(expr) == "a^_b"


def test_escape_multi_char():
    expr = 'a_b%c,d"f^g'
    assert escape_filter_expr(expr) == 'a^_b^%c^,d^"f^^g'


def test_escape_leading_char():
    expr = "-a-b"
    assert escape_filter_expr(expr) == "^-a-b"
