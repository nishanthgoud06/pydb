import pytest
from pydb.parser import parse, ParseError

def test_select():
    stmt = parse("SELECT")
    assert stmt.type == "select"
    assert stmt.row is None

def test_insert_valid():
    stmt = parse("INSERT 1 Alice 25")
    assert stmt.type == "insert"
    assert stmt.row.id == 1
    assert stmt.row.name == "Alice"
    assert stmt.row.age == 25

def test_insert_case_insensitive():
    stmt = parse("insert 1 Alice 25")
    assert stmt.type == "insert"

def test_insert_missing_args():
    with pytest.raises(ParseError):
        parse("INSERT 1")

def test_insert_invalid_id():
    with pytest.raises(ParseError):
        parse("INSERT abc Alice 25")

def test_unknown_command():
    with pytest.raises(ParseError):
        parse("DELETE 1")

def test_empty_command():
    with pytest.raises(ParseError):
        parse("")