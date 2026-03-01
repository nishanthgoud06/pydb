class Statement:
    """
    Represents a parsed command.
    type is either "insert" or "select".
    row is only set for insert statements.
    """
    def __init__(self, type: str, row=None):
        self.type = type
        self.row = row

    def __repr__(self):
        return f"Statement(type={self.type}, row={self.row})"


class ParseError(Exception):
    """Raised when a command can't be parsed."""
    pass


def parse(command: str) -> Statement:
    """
    Takes a raw string and returns a Statement object.
    Raises ParseError if the command is invalid.
    """
    # Split the command into tokens
    tokens = command.strip().split()

    if not tokens:
        raise ParseError("Empty command")

    keyword = tokens[0].upper()

    if keyword == "SELECT":
        return Statement(type="select")

    elif keyword == "INSERT":
        # INSERT expects exactly 3 arguments: id, name, age
        if len(tokens) != 4:
            raise ParseError(f"INSERT expects 3 arguments: id name age. Got {len(tokens)-1}")

        try:
            id = int(tokens[1])
        except ValueError:
            raise ParseError(f"id must be an integer, got '{tokens[1]}'")

        name = tokens[2]

        try:
            age = int(tokens[3])
        except ValueError:
            raise ParseError(f"age must be an integer, got '{tokens[3]}'")

        from pydb.pager import Row
        row = Row(id, name, age)
        return Statement(type="insert", row=row)

    else:
        raise ParseError(f"Unknown command '{tokens[0]}'. Expected SELECT or INSERT")