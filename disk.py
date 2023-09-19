from collections import OrderedDict
from abc import ABC, abstractmethod

import threading
import sqlite3

class diskset:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS elements (
                element TEXT PRIMARY KEY
            )
        """)
        self.conn.commit()
        self.length = self.size()

    def __len__(self):
        return self.length

    def __contains__(self, element):
        cursor = self.conn.execute("SELECT 1 FROM elements WHERE element = ?", (element,))
        return cursor.fetchone() is not None

    def __iter__(self):
        cursor = self.conn.execute("SELECT element FROM elements")
        return (row[0] for row in cursor)

    def add(self, element):
        self.conn.execute("INSERT OR IGNORE INTO elements (element) VALUES (?)", (element,))
        self.conn.commit()
        self.length += 1

    def extend(self, elements):
        self.conn.execute("BEGIN")
        self.conn.executemany("INSERT OR IGNORE INTO elements (element) VALUES (?)", [(element,) for element in elements])
        self.conn.commit()
        self.length += len(elements)

    def remove(self, element):
        self.conn.execute("DELETE FROM elements WHERE element = ?", (element,))
        self.conn.commit()
        self.length -= 1

    def size(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM elements")
        return cursor.fetchone()[0]

    def close(self):
        self.conn.close()

class queue(ABC):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename
        self.thread_local = threading.local()
        self.queue_lock = threading.Lock()
        self.length = self.size()

    def __len__(self):
        return self.length

    def __contains__(self, item):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = self.select(connection, item)
                return cursor.fetchone() is not None
            
    @abstractmethod
    def sample(self):
        pass

    @abstractmethod
    def select(self, connection, item):
        pass

    @abstractmethod
    def insert(self, cursor, item):
        pass

    @property
    @abstractmethod
    def initializer(self) -> str:
        pass

    def size(self):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.execute("SELECT COUNT(*) FROM queue")
                return cursor.fetchone()[0]

    def initialize_database(self, connection):
        with connection:
            cursor = connection.cursor()
            cursor.execute(self.initializer)

    def get_connection(self):
        if not hasattr(self.thread_local, "connection"):
            self.thread_local.connection = sqlite3.connect(self.filename)
            self.initialize_database(self.thread_local.connection)
        return self.thread_local.connection

    def put(self, item):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                try:
                    self.insert(cursor, item)
                except Exception:
                    cursor.execute("ROLLBACK")
                    raise

    def delete(self, cursor, id):
        cursor.execute("DELETE FROM queue WHERE id = ?", (id,))
        cursor.execute("COMMIT")
        self.length -= 1

    def get(self, block=True, delete=True):
        while True:
            with self.queue_lock:
                connection = self.get_connection()
                with connection:
                    cursor = connection.cursor()
                    cursor.execute("BEGIN")
                    try:
                        item = self.sample(cursor)
                        if item:
                            if delete:
                                self.delete(cursor, item[0])
                            if len(item) == 2:
                                return item[1]
                            else:
                                return item[1:]
                        else:
                            cursor.execute("ROLLBACK")
                            if block:
                                continue
                            else:
                                return
                    except Exception:
                        cursor.execute("ROLLBACK")
                        raise

    def close(self):
        connection = self.get_connection()
        connection.close()

class pagequeue(queue):
    initializer = """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY,
            page TEXT
        )
    """

    def select(self, connection, page):
        return connection.execute("SELECT 1 FROM queue WHERE page = ?", (page,))

    def insert(self, cursor, item):
        cursor.execute("INSERT INTO queue (page) VALUES (?)", (item,))
        cursor.execute("COMMIT")
        self.length += 1

    def sample(self, cursor):
        cursor.execute("SELECT id, page FROM queue ORDER BY id LIMIT 1")
        row = cursor.fetchone()
        return row
    
    def get_batch(self, batch_size):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                cursor.execute("SELECT page FROM queue LIMIT ?", (batch_size,))
                ids = set()
                for row in cursor.fetchall():
                    ids.add(row[0])
                cursor.execute("DELETE FROM queue WHERE page IN ({})".format(', '.join('?' * len(ids))), list(ids))
                connection.commit()
                self.length -= len(ids)
                return ids
            
    def extend(self, items):
        if not items:
            return
        
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")

                batch = [(item,) for item in items]
                cursor.executemany("INSERT OR IGNORE INTO queue (page) VALUES (?)", batch)
                connection.commit()
                self.length += len(batch)
    
    def __iter__(self):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                cursor.execute("SELECT id, page FROM queue ORDER BY id")
                for index, page in cursor.fetchall():
                    yield page

class diskqueue(queue):
    SLUG_MAPPING = OrderedDict({
        "track": 0,
        "album": 1,
        "artist": 2,
        "playlist": 3,
        "concert": 4,
        "user": 5,
        "episode": 6,
        "show": 7,
        "genre": 8,
    })

    INVERSE_SLUG_MAPPING = {value: key for key, value in SLUG_MAPPING.items()}

    initializer = """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY,
            slug INTEGER,
            hash TEXT
        )
    """
    
    def select(self, connection, item):
        return connection.execute("SELECT 1 FROM queue WHERE slug = ? AND hash = ?", item)

    def insert(self, cursor, item):
        slug, hash = item
        slug = self.SLUG_MAPPING[slug]
        cursor.execute("INSERT INTO queue (slug, hash) VALUES (?, ?)", (slug, hash))
        cursor.execute("COMMIT")
        self.length += 1

    def sample(self, cursor):
        cursor.execute("SELECT id, slug, hash FROM queue ORDER BY id LIMIT 1")
        row = cursor.fetchone()
        if row:
            id, slug, hash = row
            slug = self.INVERSE_SLUG_MAPPING[slug]
            return id, slug, hash
        else:
            return None

    def get_batch(self, batch_size):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                cursor.execute("SELECT id, slug, hash FROM queue LIMIT ?", (batch_size,))
                slugs = []
                hashes = []
                ids = set()
                for id, slug, hash in cursor.fetchall():
                    ids.add(id)
                    slugs.append(slug)#self.INVERSE_SLUG_MAPPING[slug])
                    hashes.append(hash)
                query = "DELETE FROM queue WHERE id IN ({})".format(", ".join("?" * len(ids)))
                cursor.execute(query, list(ids))
                connection.commit()
                self.length -= len(ids)

                return slugs, hashes

    def extend(self, items):
        if not items:
            return
        
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")

                #batch = [(item,) for item in items]
                cursor.executemany("INSERT OR IGNORE INTO queue (slug, hash) VALUES (?, ?)", items)
                connection.commit()
                self.length += len(items)
                
    def __iter__(self):
        with self.queue_lock:
            connection = self.get_connection()
            with connection:
                cursor = connection.cursor()
                cursor.execute("BEGIN")
                cursor.execute("SELECT id, slug, hash FROM queue ORDER BY id")
                for id, slug, hash in cursor.fetchall():
                    yield slug, hash