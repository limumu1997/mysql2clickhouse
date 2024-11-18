import sqlite3
import mysql.connector
from mysql.connector import Error
import sys
import re

def convert_sqlite_type_to_mysql(sqlite_type):
    """Convert SQLite data types to MySQL compatible types."""
    sqlite_type = sqlite_type.upper()
    type_mapping = {
        'INTEGER': 'INT',
        'REAL': 'DOUBLE',
        'TEXT': 'VARCHAR(255)',
        'BLOB': 'BLOB',
        'NULL': 'NULL'
    }
    
    # Handle VARCHAR with length
    varchar_match = re.match(r'VARCHAR\((\d+)\)', sqlite_type)
    if varchar_match:
        return f'VARCHAR({varchar_match.group(1)})'
    
    return type_mapping.get(sqlite_type, 'VARCHAR(255)')

def get_table_schema(sqlite_cursor, table_name):
    """Get table schema from SQLite database."""
    sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = sqlite_cursor.fetchall()
    
    schema = []
    primary_keys = []
    
    for col in columns:
        col_name = col[1]
        col_type = convert_sqlite_type_to_mysql(col[2])
        is_pk = col[5]  # Primary key flag
        
        # Force NOT NULL for primary key columns
        if is_pk:
            is_nullable = "NOT NULL"
            # For TEXT/BLOB columns used in primary key, ensure they're VARCHAR with length
            if col_type.upper() in ['TEXT', 'BLOB'] or col_type.upper().startswith('VARCHAR'):
                col_type = 'VARCHAR(255)'
            primary_keys.append(col_name)
        else:
            is_nullable = "NULL" if col[3] == 0 else "NOT NULL"
        
        schema.append(f"{col_name} {col_type} {is_nullable}")
    
    if primary_keys:
        schema.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
    
    return schema

def migrate_sqlite_to_mysql(sqlite_db_path, mysql_config):
    """
    Migrate data from SQLite to MySQL.
    
    Args:
        sqlite_db_path (str): Path to SQLite database file
        mysql_config (dict): MySQL connection configuration
    """
    try:
        # Connect to SQLite database
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        sqlite_cursor = sqlite_conn.cursor()
        
        # Get list of tables
        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = sqlite_cursor.fetchall()
        
        # Connect to MySQL database
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor()
        
        # Migrate each table
        for table in tables:
            table_name = table[0]
            print(f"Migrating table: {table_name}")
            
            # Get table structure details
            sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = sqlite_cursor.fetchall()
            
            # Get table schema
            schema = get_table_schema(sqlite_cursor, table_name)
            
            # Create table in MySQL
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            create_table_sql += ",\n".join(schema)
            create_table_sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            
            print(f"Creating table with SQL: {create_table_sql}")  # Debug output
            mysql_cursor.execute(create_table_sql)
            
            # Get data from SQLite
            sqlite_cursor.execute(f"SELECT * FROM {table_name}")
            rows = sqlite_cursor.fetchall()
            
            if rows:
                # Get column count for placeholder string
                column_count = len(rows[0])
                placeholders = ", ".join(["%s"] * column_count)
                
                # Insert data into MySQL
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                
                # Insert in batches to handle large datasets
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    mysql_cursor.executemany(insert_sql, batch)
                    mysql_conn.commit()
                    print(f"Inserted {len(batch)} rows into {table_name}")
            
            print(f"Completed migration of table {table_name}")
            
    except Error as e:
        print(f"Error: {e}")
        print(f"Failed SQL (if applicable): {create_table_sql}")
        sys.exit(1)
        
    finally:
        # Close connections
        if 'sqlite_conn' in locals():
            sqlite_conn.close()
        if 'mysql_conn' in locals():
            mysql_conn.close()

if __name__ == "__main__":
    # Example configuration
    mysql_config = {
        'host': '192.168.1.218',
        'user': 'root',
        'password': 'Iliot123..',
        'database': 'expressway'
    }
    
    # Example usage
    migrate_sqlite_to_mysql('C:\Projects\golang\expressway-param\data\expressway.db3', mysql_config)