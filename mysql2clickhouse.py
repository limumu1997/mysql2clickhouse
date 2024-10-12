import mysql.connector
from clickhouse_driver import Client
import pandas as pd
from datetime import datetime
import pytz

# MySQL connection details
mysql_config = {
    'host': '127.0.0.1',
    'database': 'auditdbx',
    'user': 'root',
    'password': 'Password1234'
}

# ClickHouse connection details
clickhouse_config = {
    'host': '127.0.0.1',
    'database': 'auditdbx',
    'user': 'username',
    'password': 'password'
}

def get_mysql_tables():
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

def get_mysql_schema(table_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    schema = cursor.fetchall()
    cursor.close()
    conn.close()
    return schema

def get_mysql_indices(table_name):
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()
    cursor.execute(f"SHOW INDEX FROM {table_name}")
    indices = cursor.fetchall()
    cursor.close()
    conn.close()
    return indices

def mysql_to_clickhouse_type(mysql_type):
    type_mapping = {
        'tinyint': 'Int8',
        'smallint': 'Int16',
        'mediumint': 'Int32',
        'int': 'Int32',
        'bigint': 'Int64',
        'float': 'Float32',
        'double': 'Float64',
        'decimal': 'Decimal',
        'char': 'String',
        'varchar': 'String',
        'text': 'String',
        'date': 'Date',
        'datetime': 'DateTime',
        'timestamp': 'DateTime',
        'boolean': 'UInt8',
        'enum': 'Enum8',
        'set': 'Array(String)',
        'json': 'String'
    }
    
    for mysql_prefix, ch_type in type_mapping.items():
        if mysql_type.lower().startswith(mysql_prefix):
            if mysql_prefix == 'decimal':
                precision, scale = map(int, mysql_type[8:-1].split(','))
                return f'Decimal({precision},{scale})'
            return ch_type
    
    return 'String'  # Default to String for unknown types

def create_clickhouse_table(client, table_name, schema, indices, allow_nullable_key=False):
    columns = []
    primary_key = None
    nullable_columns = set()
    for col in schema:
        name, type_, null, key, default, extra = col
        ch_type = mysql_to_clickhouse_type(type_)
        if null == 'YES':
            ch_type = f'Nullable({ch_type})'
            nullable_columns.add(name)
        columns.append(f'{name} {ch_type}')
        if key == 'PRI':
            primary_key = name

    columns_str = ', '.join(columns)
    
    # Determine ORDER BY clause based on MySQL indices, excluding the auto-increment primary key
    order_by_columns = set()
    for index in indices:
        if index[2] != 'PRIMARY' and index[4] != primary_key:
            order_by_columns.add(index[4])
    
    # Remove nullable columns from ORDER BY if allow_nullable_key is False
    if not allow_nullable_key:
        order_by_columns = order_by_columns - nullable_columns
    
    # If no suitable indices found, use a non-nullable timestamp column if available, otherwise use tuple()
    if not order_by_columns:
        timestamp_column = next((col[0] for col in schema if col[1].lower() in ['timestamp', 'datetime'] and col[0] not in nullable_columns), None)
        order_by_str = timestamp_column if timestamp_column else 'tuple()'
    else:
        order_by_str = ', '.join(order_by_columns)
    
    settings = "SETTINGS allow_nullable_key=1" if allow_nullable_key else ""
    
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_str}
    ) ENGINE = MergeTree()
    ORDER BY ({order_by_str})
    {settings}
    """
    client.execute(create_query)
    print(f"Created ClickHouse table with ORDER BY ({order_by_str})")

def sync_data(mysql_conn, ch_client, table_name, batch_size=10000):
    cursor = mysql_conn.cursor(dictionary=True)
    offset = 0
    
    while True:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {offset}, {batch_size}")
        batch = cursor.fetchall()
        if not batch:
            break
        
        df = pd.DataFrame(batch)
        data = [tuple(x) for x in df.to_numpy()]
        ch_client.execute(f'INSERT INTO {table_name} VALUES', data)
        
        offset += batch_size
        print(f"Inserted {len(batch)} rows into ClickHouse")

def sync_table(table_name, allow_nullable_key=False):
    print(f"Syncing table: {table_name}")
    
    # Get MySQL schema and indices
    mysql_schema = get_mysql_schema(table_name)
    mysql_indices = get_mysql_indices(table_name)
    
    # Create ClickHouse table with index information
    ch_client = Client(**clickhouse_config)
    create_clickhouse_table(ch_client, table_name, mysql_schema, mysql_indices, allow_nullable_key)
    
    # Sync data
    mysql_conn = mysql.connector.connect(**mysql_config)
    sync_data(mysql_conn, ch_client, table_name)
    
    mysql_conn.close()
    print(f"Sync completed for table: {table_name}")

def main():
    tables = get_mysql_tables()
    
    for table in tables:
        try:
            sync_table(table, allow_nullable_key=False)
        except Exception as e:
            print(f"Error syncing table {table}: {str(e)}")
            
            # If the error is due to nullable key, try again with allow_nullable_key=True
            if "Sorting key contains nullable columns" in str(e):
                print(f"Retrying table {table} with allow_nullable_key=True")
                try:
                    sync_table(table, allow_nullable_key=True)
                except Exception as e2:
                    print(f"Error syncing table {table} even with allow_nullable_key=True: {str(e2)}")
            
    print("Sync process completed for all tables")

if __name__ == "__main__":
    main()