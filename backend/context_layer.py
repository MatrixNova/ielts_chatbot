import logging
import config
from pinecone import Pinecone

logger = logging.getLogger(config.APP_NAME)

# Set context relevance for Pinecone
relevance_threshold = 0.85  

# Queries Pinecone and returns results with scores
def query_pinecone(query: str, pc: Pinecone, index, top_k=3):
    try:
        embedding_responses = pc.inference.embed(
            model=config.PINECONE_INDEX_MODEL,
            inputs=[query],
            parameters={"input_type": "query"}
        )
        query_embedding = embedding_responses.data[0].embedding
        query_responses = index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True,
            namespace=config.PINECONE_NAMESPACE
        )
        return [{"text": match["metadata"]["text"], "score": match["score"]} for match in query_responses["matches"]]
    except Exception as e:
        logger.error("Pinecone query failed: %s", e, exc_info=True)
        return []

def get_context_for_query(query: str, pc: Pinecone, index) -> str:
    """
    Main function to get the best context for a query.
    It queries Pinecone and decides whether to use the results based on a relevance threshold.
    Returns a context string or an empty string.
    """
    pinecone_results = query_pinecone(query, pc, index)

    if not pinecone_results:
        logger.warning("No results returned from Pinecone for query: %s", query)
        return ""

    top_score = pinecone_results[0]["score"]
    
    if top_score >= relevance_threshold:
        logger.info(f"Top score {top_score:.2f} meets threshold. Using retrieved context.")
        passage_texts = [res["text"] for res in pinecone_results]
        return " ".join(passage_texts)
    else:
        logger.warning(f"Top score {top_score:.2f} is below threshold {relevance_threshold}. Discarding context.")
        return ""