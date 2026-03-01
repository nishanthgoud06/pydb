# pager.py
import os
import struct

PAGE_SIZE = 4096  # 4KB — same default as SQLite
ROW_FORMAT = "=i10si"       # int, 10-char string, int
ROW_SIZE = struct.calcsize(ROW_FORMAT)  # automatically calculates 18 or 20 bytes

class Row:
    def __init__(self, id: int, name: str, age: int):
        self.id = id
        self.name = name
        self.age = age

    def to_bytes(self) -> bytes:
        # Encode name to bytes, trimmed/padded to 10 chars
        name_bytes = self.name.encode("utf-8")[:10].ljust(10, b"\x00")
        return struct.pack(ROW_FORMAT, self.id, name_bytes, self.age)

    @classmethod
    def from_bytes(cls, data: bytes):
        id, name_bytes, age = struct.unpack(ROW_FORMAT, data)
        name = name_bytes.decode("utf-8").rstrip("\x00")  # remove padding zeros
        return cls(id, name, age)

    def __repr__(self):
        return f"Row(id={self.id}, name={self.name}, age={self.age})"

class Pager:
    """
    Manages reading and writing fixed-size pages to a file.
    This is the ONLY layer that talks to disk. Everything else
    in our database will ask the Pager for pages — it never
    touches the file directly.
    """

    def __init__(self, filepath: str):
        # 'r+b' = read+write in binary mode, file must exist
        # 'w+b' = read+write in binary mode, creates or truncates
        # We use binary mode because we're managing raw bytes, not text
        if os.path.exists(filepath):
            self.file = open(filepath, 'r+b')
        else:
            self.file = open(filepath, 'w+b')

        self.filepath = filepath

        # A simple in-memory cache: page_number -> bytes
        # This avoids hitting disk every time we need a page
        self.cache: dict[int, bytearray] = {}

    def get_page(self, page_num: int) -> bytearray:
        """
        Returns a page as a bytearray.
        Checks cache first. If not cached, reads from disk.
        If page doesn't exist yet, returns a blank page.
        """
        if page_num in self.cache:
            return self.cache[page_num]

        # Calculate exactly where on disk this page lives
        offset = page_num * PAGE_SIZE

        # Seek to that position and read exactly PAGE_SIZE bytes
        self.file.seek(offset)
        data = self.file.read(PAGE_SIZE)

        if data:
            # bytearray is mutable (unlike bytes) — we need to modify pages
            page = bytearray(data)
        else:
            # Page doesn't exist on disk yet — start blank
            page = bytearray(PAGE_SIZE)  # PAGE_SIZE zero bytes

        self.cache[page_num] = page
        return page

    def write_page(self, page_num: int, data: bytearray):
        """
        Writes a page to the cache AND to disk immediately.
        """
        assert len(data) == PAGE_SIZE, f"Page must be exactly {PAGE_SIZE} bytes"

        self.cache[page_num] = data

        offset = page_num * PAGE_SIZE
        self.file.seek(offset)
        self.file.write(data)
        self.file.flush()  # Force OS to write to disk, not just its own buffer

    def close(self):
        self.file.close()

class Table:
    """
    Manages storing and retrieving rows inside pages.
    Knows how to calculate which page a row lives on
    and where inside that page.
    """

    def __init__(self, pager: Pager):
        self.pager = pager
        self.meta_file = pager.filepath + ".meta"

        # Load num_rows from meta file if it exists
        if os.path.exists(self.meta_file):
            with open(self.meta_file, "r") as f:
                self.num_rows = int(f.read().strip())
        else:
            self.num_rows = 0

    def _save_meta(self):
        with open(self.meta_file, "w") as f:
            f.write(str(self.num_rows))

    def row_slot(self, row_num: int):
        """
        Given a row number, returns (page, offset_inside_page).
        This is the core calculation — where does row N live?
        """
        rows_per_page = PAGE_SIZE // ROW_SIZE
        page_num = row_num // rows_per_page
        row_offset = (row_num % rows_per_page) * ROW_SIZE
        page = self.pager.get_page(page_num)
        return page, row_offset

    def insert(self, row: Row):
        page, offset = self.row_slot(self.num_rows)
        row_bytes = row.to_bytes()
        page[offset: offset + ROW_SIZE] = row_bytes
        self.pager.write_page(
            self.num_rows // (PAGE_SIZE // ROW_SIZE), page
        )
        self.num_rows += 1
        self._save_meta()  # persist num_rows after every insert

    def select(self) -> list:
        rows = []
        for i in range(self.num_rows):
            page, offset = self.row_slot(i)
            row = Row.from_bytes(page[offset: offset + ROW_SIZE])
            rows.append(row)
        return rows