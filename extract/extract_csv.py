import pandas as pd
import ssl


ssl._create_default_https_context = ssl._create_unverified_context

def extract_tree_csv_data():
    base_url = "https://data.cityofnewyork.us/resource/uvpi-gqnh.csv"
    limit = 50000
    offset = 0
    all_chunks = []

    while True:
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        print(f"Fetching rows {offset} to {offset + limit}...")
        
        chunk = pd.read_csv(url)
        
        if chunk.empty:
            break
        
        all_chunks.append(chunk)
        offset += limit

    #df = pd.read_csv(f"{base_url}?$limit=5000")
    # combine all chunks into one df
    df = pd.concat(all_chunks, ignore_index=True)
    print(f"Total rows retrieved: {len(df)}")
    print("First 5 entries:")
    print(df.head(5))

    return df


