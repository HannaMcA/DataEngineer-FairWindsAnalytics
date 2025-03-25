from dotenv import load_dotenv
from extract.extract_csv import extract_tree_csv_data
from extract.extract_json import extract_weather_json_data
from vector_upsert import upsert_to_pinecone

load_dotenv()


def main():
    index_name = "tree-embeddings"

    # extract tree csv data
    df = extract_tree_csv_data()

    # upsert to pinecone
    index, model = upsert_to_pinecone(df, index_name)

    query_embedding = model.encode(["Norway maple Poor Dead"])
    result = index.query(
        vector=query_embedding.tolist(), 
        top_k=5, 
        include_metadata=False
    )

    # extract id, map to df and display
    similar_ids = [match['id'] for match in result['matches']]
    similar_ids = list(map(int, similar_ids))
    similar_trees = df.loc[similar_ids]
    print(similar_trees[["spc_common", "health", "status", "text"]])

    # weather_df = extract_weather_data()

if __name__ == "__main__":
    main()





    