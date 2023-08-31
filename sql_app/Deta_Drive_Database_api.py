import io
import random
from deta import Deta
import sqlite3

class DetaDriveDatabase:
    def __init__(self, deta_project_key, drive_name, db_name):
        # Initialize the Deta library with the provided project key
        self.deta = Deta(deta_project_key)
        # Get a reference to the specified Deta Drive
        self.drive = self.deta.Drive(drive_name)
        # Store the database name for later use
        self.db_name = db_name
        # Initialize sthe database connection and cursor to None
        self.conn = None
        self.cursor = None

    def connect(self):
        # Check if the database file exists on Deta Drive
        db_file = self.drive.get(self.db_name)

        try:
            db_read = db_file.read()
        except AttributeError:
            db_read = None
        
            
        if db_read == None or 0 == len(db_read):
            # Create a new empty in-memory SQLite database
            conn = sqlite3.connect(":memory:")
            # Dump the contents of the in-memory database to a new BytesIO object
            db_file_new = io.BytesIO()
            for line in conn.iterdump():
                db_file_new.write(f"{line}\n".encode("utf-8"))
            db_file_new.seek(0)
            # Upload the new database file to Deta Drive
            self.drive.put(self.db_name, db_file_new)
            db_file = self.drive.get(self.db_name)
            
            if db_read == None:
                print(f"The database file `{self.db_name}` does not exist in the Deta drive, create a new one")
                db_read = db_file.read()
            

        # Read the database file from Deta Drive as bytes
        db_bytes = db_read
        db_file = io.BytesIO(db_bytes)

        # Create an in-memory SQLite database
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()

        # Load the contents of the db_file object into the in-memory database
        self.conn.executescript(db_file.getvalue().decode("utf-8"))

    def close(self):
        # Dump the contents of the in-memory database to a new BytesIO object
        db_file_new = io.BytesIO()
        for line in self.conn.iterdump():
            db_file_new.write(f"{line}\n".encode("utf-8"))

        # Upload the updated database file to Deta Drive
        db_file_new.seek(0)
        self.drive.put(self.db_name, db_file_new)

        # Close the database connection and set it to None
        self.conn.close()
        self.conn = None

    def save(self):
        
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")


        # Dump the contents of the in-memory database to a new BytesIO object
        db_file_new = io.BytesIO()
        for line in self.conn.iterdump():
            db_file_new.write(f"{line}\n".encode("utf-8"))

        # Upload the updated database file to Deta Drive
        db_file_new.seek(0)
        self.drive.put(self.db_name, db_file_new)
    
    def load_db_to_memory(self, dbapi_con):
        # Load the database from Deta Drive
        data = self.drive.get(self.db_name)
        if data:
            # Load the database into memory
            dbapi_con.executescript(data.read().decode())

    def save_memory_to_drive(self, dbapi_con):
        # Save the in-memory database to a file
        script = "".join(dbapi_con.iterdump())
        # Save the file to your Deta Drive
        self.drive.put(self.db_name, script.encode())
    
    def delete_database(self):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Close the database connection and set it to None
        self.conn.close()
        self.conn = None
        
        # Delete the database file from Deta Drive
        self.drive.delete(self.db_name)
        
    def clear_table(self, table_name):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Execute the DELETE FROM SQL statement to delete all rows from the table
        self.cursor.execute(f"DELETE FROM {table_name}")
        
    def clear_database(self):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Get a list of all tables in the database
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in self.cursor.fetchall()]
        
        # Truncate each table
        for table in tables:
            self.truncate_table(table)
        
    def create_table(self, table_name, columns):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Create a string representation of the columns and their data types
        columns_str = ", ".join([f"{name} {data_type}" for name, data_type in columns.items()])
        # Execute the CREATE TABLE SQL statement
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})")

    def drop_table(self, table_name):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Execute the DROP TABLE SQL statement
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    def rename_table(self, old_table_name, new_table_name):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Execute the ALTER TABLE RENAME TO SQL statement
        self.cursor.execute(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")

    def insert_data(self, table_name, columns, values):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Create a string representation of the columns and placeholders for the values
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        # Execute the INSERT INTO SQL statement for each set of values
        self.cursor.executemany(query, values)

    def update_data(self, table_name, set_values, where_condition):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Create a string representation of the SET clause
        set_values_str = ", ".join([f"{column} = ?" for column in set_values.keys()])
        # Create a string representation of the WHERE clause
        where_condition_str = " AND ".join([f"{column} = ?" for column in where_condition.keys()])
        # Combine the values from the SET and WHERE clauses into a single list
        values = list(set_values.values()) + list(where_condition.values())
        # Execute the UPDATE SQL statement
        self.cursor.execute(f"UPDATE {table_name} SET {set_values_str} WHERE {where_condition_str}", values)

    def delete_data(self, table_name, where_condition):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Create a string representation of the WHERE clause
        where_condition_str = " AND ".join([f"{column} = ?" for column in where_condition.keys()])
        values = list(where_condition.values())
        # Execute the DELETE FROM SQL statement
        self.cursor.execute(f"DELETE FROM {table_name} WHERE {where_condition_str}", values)

    def select_data(self, table_name, columns="*", where_condition=None):
        # Check if the database is connected
        if not self.conn:
            raise Exception("Database is not connected. Please call the connect method first.")
        
        # Create a string representation of the columns to select
        columns_str = ", ".join(columns) if isinstance(columns, list) else columns
        if where_condition:
            # Create a string representation of the WHERE clause
            where_condition_str = " AND ".join([f"{column} = ?" for column in where_condition.keys()])
            values = list(where_condition.values())
            # Execute the SELECT SQL statement with a WHERE clause
            self.cursor.execute(f"SELECT {columns_str} FROM {table_name} WHERE {where_condition_str}", values)
        else:
            # Execute the SELECT SQL statement without a WHERE clause
            self.cursor.execute(f"SELECT {columns_str} FROM {table_name}")
        
        # Return all rows from the SELECT statement
        return self.cursor.fetchall()

# Example usage
if __name__ == "__main__":
    
    deta_project_key = "YOUR_DETA_PROJECT_KEY" # or collections
    drive_name = "YOUR_DETA_DRIVE_NAME"
    db_name = "YOUR_DATABASE_NAME.db"
    

    db = DetaDriveDatabase(deta_project_key, drive_name, db_name)
    db.connect()

    # Create a new table
    table_name = "items"
    columns = {"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT", "value": "INTEGER"}
    db.create_table(table_name, columns)
    
    # Insert data into the database
    table_name = "items"
    columns = ["name", "value"]
    values = [(f"item-{i}", random.randint(1, 100)) for i in range(5)]
    db.insert_data(table_name, columns, values)
    
    # # Insert data into the database
    # table_name = "items"
    # columns = ["name", "value"]
    # values = [("apple", 10), ("banana", 20), ("orange", 30)]
    # db.insert_data(table_name, columns, values)
    
    # Update some data in the database
    set_values = {"value": 200}
    where_condition = {"name": "item-0"}
    db.update_data(table_name, set_values, where_condition)

    # Delete some data from the database
    where_condition = {"name": "item-1"}
    db.delete_data(table_name, where_condition)

    # Select some data from the database
    result = db.select_data(table_name)
    print(result)

    # Close the database connection
    db.close()

