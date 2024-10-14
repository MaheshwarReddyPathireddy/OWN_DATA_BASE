import os
import json
from typing import List, Dict, Any

class Table:
    def __init__(self, name: str, columns: List[str]):
        self.name = name
        self.columns = columns
        self.rows: List[Dict[str, Any]] = []

class Database:
    def __init__(self, name: str):
        self.name = name
        self.tables: Dict[str, Table] = {}

    def create_table(self, table_name: str, columns: List[str]):
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, columns)
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")

    def insert(self, table_name: str, data: Dict[str, Any]):
        if table_name in self.tables:
            table = self.tables[table_name]
            if set(data.keys()) == set(table.columns):
                table.rows.append(data)
                print("Data inserted successfully.")
            else:
                print("Column mismatch. Insertion failed.")
        else:
            print(f"Table '{table_name}' does not exist.")

    def select(self, table_name: str, conditions: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if table_name in self.tables:
            table = self.tables[table_name]
            if conditions:
                return [row for row in table.rows if all(row.get(k) == v for k, v in conditions.items())]
            else:
                return table.rows
        else:
            print(f"Table '{table_name}' does not exist.")
            return []

    def save(self):
        if not os.path.exists(self.name):
            os.makedirs(self.name)
        for table_name, table in self.tables.items():
            with open(f"{self.name}/{table_name}.json", 'w') as f:
                json.dump({'columns': table.columns, 'rows': table.rows}, f)
        print("Database saved successfully.")

    def load(self):
        if os.path.exists(self.name):
            for filename in os.listdir(self.name):
                if filename.endswith('.json'):
                    table_name = filename[:-5]
                    with open(f"{self.name}/{filename}", 'r') as f:
                        data = json.load(f)
                        self.tables[table_name] = Table(table_name, data['columns'])
                        self.tables[table_name].rows = data['rows']
            print("Database loaded successfully.")
        else:
            print("No existing database found.")

# Simple query parser
def parse_query(query: str):
    tokens = query.split()
    if tokens[0].upper() == "SELECT":
        table_name = tokens[3]
        conditions = {}
        if "WHERE" in tokens:
            where_index = tokens.index("WHERE")
            for i in range(where_index + 1, len(tokens), 4):
                conditions[tokens[i]] = tokens[i + 2]
        return ("SELECT", table_name, conditions)
    elif tokens[0].upper() == "INSERT":
        table_name = tokens[2]
        data = json.loads(" ".join(tokens[4:]))
        return ("INSERT", table_name, data)
    else:
        return None

# Usage example
if __name__ == "__main__":
    db = Database("mydb")
    db.create_table("users", ["id", "name", "age"])
    db.insert("users", {"id": 1, "name": "Alice", "age": 30})
    db.insert("users", {"id": 2, "name": "Bob", "age": 25})

    print(db.select("users", {"age": 30}))

    db.save()

    # Simple query execution
    query = "SELECT * FROM users WHERE age = 30"
    operation, table, conditions = parse_query(query)
    if operation == "SELECT":
        result = db.select(table, conditions)
        print(result)