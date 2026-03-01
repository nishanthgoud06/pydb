from pydb.pager import Pager, Table
from pydb.parser import parse, ParseError

def start(db_file: str = "mydb.db"):
    pager = Pager(db_file)
    table = Table(pager)

    print(f"PyDB started. Using '{db_file}'")
    print("Commands: INSERT <id> <name> <age> | SELECT | exit")

    while True:
        try:
            command = input("pydb> ").strip()
        except (KeyboardInterrupt, EOFError):
            # Handle Ctrl+C and Ctrl+D cleanly
            print("\nBye!")
            pager.close()
            break

        if not command:
            continue

        if command.lower() == "exit":
            print("Bye!")
            pager.close()
            break

        try:
            statement = parse(command)

            if statement.type == "insert":
                table.insert(statement.row)
                print("OK")

            elif statement.type == "select":
                rows = table.select()
                if not rows:
                    print("(empty)")
                for row in rows:
                    print(row)

        except ParseError as e:
            print(f"Error: {e}")