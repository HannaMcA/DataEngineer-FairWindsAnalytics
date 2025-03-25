from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec
import os

# slice into fixed chunks
def batch_generator(items, batch_size):
        for i in range(0, len(items), batch_size):
            yield items[i:i+batch_size]

def upsert_to_pinecone(df, index_name, batch_size=100):
    # init pinecone
    model = SentenceTransformer("all-MiniLM-L6-v2") 
    pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
    
    # reset index if created
    if pc.has_index(index_name):
        pc.delete_index(index_name)

    # create index
    pc.create_index(
        name=index_name, 
        dimension=384,
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        ) 
    )

    index = pc.Index(index_name)

    df["text"] = df["spc_common"].fillna("") + " " + df["health"].fillna("") + " " + df["status"].fillna("")


    # convert index to string for pinecone. ignore empty rows, store as (id, text) tuples
    data = [(str(idx), row) for idx, row in zip(df.index, df["text"]) if row.strip() != ""]

    # encode and upsert in batches
    for batch in batch_generator(data, batch_size=100):
        ids, texts = zip(*batch)
        embeddings = model.encode(texts, show_progress_bar=False)

        #dict format pinecone v3 expects
        vectors = [
            {"id": id_, "values": emb.tolist()}
            for id_, emb in zip(ids, embeddings)
        ]

        index.upsert(vectors=vectors)
        print(f"Uploaded {len(vectors)} vectors")

    return index, model

