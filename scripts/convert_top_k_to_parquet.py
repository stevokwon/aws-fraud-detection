import pandas as pd

def convert_csv_to_parquet(csv_path, parquet_path):
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index = False)
    print(f"Saved parquet to {parquet_path}")

if __name__ == '__main__':
    convert_csv_to_parquet(
        csv_path = 'metadata/top_k_flagged.csv',
        parquet_path = 'metadata/top_k_flagged.parquet'
    )