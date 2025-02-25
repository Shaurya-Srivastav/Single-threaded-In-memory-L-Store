# # lstore/db.py

# import os
# import json
# from lstore.table import Table
# from lstore.bufferpool import BufferPool
# from lstore.config import DATA_PATH

# class Database:
#     def __init__(self):
#         self.tables = {}
#         self.bufferpool = None
#         self.db_path = DATA_PATH

#     def open(self, path):
#         """
#         Initialize bufferpool, read metadata from disk, re-construct tables.
#         """
#         self.db_path = path
#         self.bufferpool = BufferPool(self.db_path)
#         os.makedirs(self.db_path, exist_ok=True)

#         meta_file = os.path.join(self.db_path, "db_meta.json")
#         if os.path.exists(meta_file):
#             with open(meta_file, "r") as f:
#                 meta = json.load(f)
#             for tbl_name, tbl_info in meta.items():
#                 t = Table(tbl_name, tbl_info["num_columns"], tbl_info["key"], self.bufferpool)
#                 t.next_rid = tbl_info["next_rid"]
#                 # load more info if needed
#                 self.tables[tbl_name] = t
#         else:
#             # no metadata file => no existing tables
#             pass

#     def close(self):
#         """
#         Flush all pages, write metadata to disk, etc.
#         """
#         if self.bufferpool:
#             self.bufferpool.flush_all()

#         # write metadata
#         meta = {}
#         for tbl_name, tbl_obj in self.tables.items():
#             meta[tbl_name] = {
#                 "num_columns": tbl_obj.num_columns,
#                 "key": tbl_obj.key,
#                 "next_rid": tbl_obj.next_rid
#             }
#         meta_file = os.path.join(self.db_path, "db_meta.json")
#         with open(meta_file, "w") as f:
#             json.dump(meta, f, indent=2)

#     def create_table(self, name, num_columns, key_index):
#         # create table object
#         t = Table(name, num_columns, key_index, self.bufferpool)
#         self.tables[name] = t
#         return t

#     def drop_table(self, name):
#         if name in self.tables:
#             # optionally remove files from disk
#             self.tables[name].drop_files()
#             del self.tables[name]

#     def get_table(self, name):
#         return self.tables.get(name, None)



import os
import msgpack
from lstore.table import Table, Record
from lstore.index import Index
import pickle
from lstore.bufferpool import Bufferpool
INDEX_EXT_CODE = 1
TABLE_EXT_CODE = 2

def convert_to_serializable(obj, seen=None):
    """
    Recursively converts custom objects (like Table or Index) into
    plain Python types (dict, list, etc.) suitable for msgpack.
    Uses a seen set (tracking object ids) to avoid infinite recursion.
    """
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        # Instead of recursing indefinitely, return a marker.
        # You might also choose to raise an error or handle references specially.
        return f"<recursion: {type(obj).__name__}>"
    seen.add(obj_id)

    # Basic types can be returned as is.
    if isinstance(obj, (str, int, float, bool, type(None))):
        seen.remove(obj_id)
        return obj

    # Handle dictionaries.
    if isinstance(obj, dict):
        result = {convert_to_serializable(k, seen): convert_to_serializable(v, seen) 
                  for k, v in obj.items()}
        seen.remove(obj_id)
        return result

    # Handle lists, tuples, and sets.
    if isinstance(obj, (list, tuple, set)):
        # Convert sets to lists for serialization.
        result = [convert_to_serializable(item, seen) for item in obj]
        seen.remove(obj_id)
        return result

    # For your custom types, tag them with a marker so you can rehydrate later.
    if isinstance(obj, Table):
        result = {
            "__custom_type__": "Table",
            "state": convert_to_serializable(obj.__dict__, seen)
        }
        seen.remove(obj_id)
        return result

    if isinstance(obj, Index):
        result = {
            "__custom_type__": "Index",
            "state": convert_to_serializable(obj.__dict__, seen)
        }
        seen.remove(obj_id)
        return result

    # For any other objects that have a __dict__, try converting that.
    if hasattr(obj, '__dict__'):
        result = {
            "__custom_type__": type(obj).__name__,
            "state": convert_to_serializable(obj.__dict__, seen)
        }
        seen.remove(obj_id)
        return result

    # Fallback: return the string representation.
    seen.remove(obj_id)
    return str(obj)

def rehydrate(obj):
    """
    Recursively rehydrates objects that were tagged during serialization.
    """
    if isinstance(obj, dict):
        if "__custom_type__" in obj:
            typ = obj["__custom_type__"]
            state = obj.get("state", {})
            # Rehydrate a Table object.
            if typ == "Table":
                instance = Table()  # Assumes Table() can be called without arguments.
                instance.__dict__.update(rehydrate(state))
                return instance
            # Rehydrate an Index object.
            if typ == "Index":
                instance = Index()  # Assumes Index() can be called without arguments.
                instance.__dict__.update(rehydrate(state))
                return instance
            # For other custom types, you could add additional logic.
        # Otherwise, process keys and values.
        return {k: rehydrate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [rehydrate(item) for item in obj]
    return obj





class Database:
    def __init__(self, bufferpool_size=10):
        self.tables = {}  # Stores all tables
        self.db_path = None  # Database directory for persistence
        self.bufferpool = Bufferpool(bufferpool_size)  # Bufferpool for managing pages

    def open(self, path):
        """Loads database from disk, including persisted tables."""
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path)
            print("Database directory created.")
            return

        self.tables = {}
        for filename in os.listdir(path):
            if filename.endswith(".tbl"):
                table_name = filename[:-4]
                file_path = os.path.join(path, filename)
                with open(file_path, "rb") as f:
                    data = f.read()
                    if not data:
                        print(f"Warning: {filename} is empty. Skipping.")
                        continue
                    raw_state = msgpack.unpackb(data, raw=False)
                # Rehydrate custom objects in the unpacked state.
                table = rehydrate(raw_state)
                self.tables[table_name] = table

        print("Database loaded from disk.")

    def close(self):
        """Saves all tables and their data to disk."""
        if not self.db_path:
            raise ValueError("Database path is not set.")

        for table_name, table in self.tables.items():
            file_path = os.path.join(self.db_path, f"{table_name}.tbl")
            with open(file_path, "wb") as f:
                # Convert the entire table to a serializable structure.
                serializable_state = convert_to_serializable(table)
                packed = msgpack.packb(serializable_state, use_bin_type=True)
                f.write(packed)




    def create_table(self, name, num_columns, key_index):
        """Creates a new table."""
        table = Table(name, num_columns, key_index)  # ✅ Ensure key_index is passed correctly
        self.tables[name] = table
        return table


    def drop_table(self, name):
        """Deletes a table."""
        if name in self.tables:
            del self.tables[name]
            os.remove(os.path.join(self.db_path, f"{name}.tbl"))

    def get_table(self, name):
        """Retrieves a table."""
        return self.tables.get(name, None)

def default(obj):
    # Check for your custom Table type first
    if isinstance(obj, Table):
        state = obj.__dict__.copy()
        # Remove the attribute that causes recursion
        state.pop('db', None)
        return state
    # For other objects, do a similar check
    if hasattr(obj, '__dict__'):
        state = obj.__dict__.copy()
        # Remove potential recursive keys if necessary
        state.pop('db', None)
        return state
    raise TypeError(f"Object of type {type(obj)} is not serializable")
