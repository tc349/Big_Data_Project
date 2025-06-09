import pandas as pd
import os


input_file = 'data/raw/synthetic_fraud_dataset.csv'
output_dir = 'data/split'
os.makedirs(output_dir, exist_ok=True)


df = pd.read_csv(input_file)

# Validate length
if len(df) != 50000:
    raise ValueError(f"Expected 50,000 rows, found {len(df)} rows.")


df_batch = df.iloc[:30000]
df_incremental = df.iloc[30000:40000]
df_kafka = df.iloc[40000:]


df_batch.to_csv(f'{output_dir}/full_load.csv', index=False)
df_incremental.to_csv(f'{output_dir}/incremental_load.csv', index=False)
df_kafka.to_csv(f'{output_dir}/kafka_streaming.csv', index=False)

print("CSV successfully split and saved to data/split/")


