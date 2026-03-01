from pydb.pager import Pager, Row, Table
import os

# Clean up old test file
if os.path.exists("test.db"):
    os.remove("test.db")

pager = Pager("test.db")
table = Table(pager)

# Insert some rows
table.insert(Row(1, "Alice", 25))
table.insert(Row(2, "Bob", 30))
table.insert(Row(3, "Charlie", 22))

# Read them back
rows = table.select()
for row in rows:
    print(row)

pager.close()