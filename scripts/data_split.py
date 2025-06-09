import pandas as pd
import os


input_file = 'data/raw/synthetic_fraud_dataset.csv'
output_dir = 'data/split'
os.makedirs(output_dir, exist_ok=True)


df = pd.read_csv(input_file)
total_rows = len(df)

# Validate length
full_load_end = int(total_rows * 0.60)
incremental_end = full_load_end + int(total_rows * 0.20)


df_batch = df.iloc[:full_load_end]
df_incremental = df.iloc[full_load_end:incremental_end]
df_kafka = df.iloc[incremental_end:]


df_batch.to_csv(f'{output_dir}/full_load.csv', index=False)
df_incremental.to_csv(f'{output_dir}/incremental_load.csv', index=False)
df_kafka.to_csv(f'{output_dir}/kafka_streaming.csv', index=False)

print(f" Split complete:")
print(f" - Full Load (60%): {len(df_batch)} rows")
print(f" - Incremental (20%): {len(df_incremental)} rows")
print(f" - Kafka (20%): {len(df_kafka)} rows")


