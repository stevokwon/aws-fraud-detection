import pyarrow.parquet as pq
import pandas as pd

def generate_athena_ddl(
        parquet_file_path,
        table_name,
        s3_location
):
    table = pd.read_table(parquet_file_path)
    schema = table.schema

    athena_types = {
        'int64' : 'BIGINT',
        'float64' : 'DOUBLE',
        'string' : 'STRING',
        'timestamp[ns]' : 'TIMESTAMP'
    }

    columns = []
    for field in schema:
        field_name = field.name
        field_type = athena_types.get(str(field.type), 'STRING')
        columns.append(f"    {field_name} {field_type}")

    columns_str = ",\n".join(columns)

    ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
{columns_str}
)    
STORED AS PARQUET
LOCATION '{s3_location}'
TBLPROPERTIES ('has_encrypted_data' = 'false');
"""
    return ddl

if __name__ == '__main__':
    ddl = generate_athena_ddl(
        parquet_file_path=""
    )