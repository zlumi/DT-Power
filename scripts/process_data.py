import pandas as pd
import os
import pickle
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
results_dir = os.path.join(project_root, 'results_1min')
pickle_dir = os.path.join(project_root, 'pickles', '1min')

os.makedirs(pickle_dir, exist_ok=True)

file_types = [
    'agg_open_interest',
    'open_interest',
    'transaction_hist',
    'transaction'
]

data_dicts = {ftype: {} for ftype in file_types}

if os.path.exists(results_dir):
    for filename in os.listdir(results_dir):
        if not filename.endswith('.csv'):
            continue

        base_name, _ = os.path.splitext(filename)
        
        for ftype in file_types:
            if base_name.endswith(f'_{ftype}'):
                file_path = os.path.join(results_dir, filename)
                
                try:
                    df = pd.read_csv(file_path)
                    df = df.loc[:, ~df.columns.str.contains('^Unnamed')] # Drop unnamed columns
                    
                    # Extract timestamp from filename and add as Trading_time column
                    timestamp_str = base_name.split('_')[0]
                    trading_time = datetime.strptime(timestamp_str, "%Y%m%d-%H%M%S").strftime("%Y-%m-%d %H:%M:%S+00:00")
                    df['Trading_time'] = trading_time
                    
                    data_dicts[ftype][timestamp_str] = df
                    print(f"Processed {filename}")
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
                break # Move to next file once type is matched

for ftype, data_dict in data_dicts.items():
    if data_dict:
        output_filename = f'all_{ftype}.pkl'
        output_path = os.path.join(pickle_dir, output_filename)
        with open(output_path, 'wb') as f:
            pickle.dump(data_dict, f)
        print(f"Saved data to {output_path}")

print("Processing complete.")