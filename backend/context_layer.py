import logging
import config
from pinecone import Pinecone

logger = logging.getLogger(config.APP_NAME)

# Set context relevance for Pinecone
relevance_threshold = 0.85  

def query_pinecone(query: str, pc: Pinecone, index, top_k=3):
    """Queries Pinecone and returns results with scores."""
    try:
        embedding_responses = pc.inference.embed(
            model=config.PINECONE_INDEX_MODEL,
            inputs=[query],
            parameters={"input_type": "query"}
        )
        
        # Ensure the response structure is valid
        if not hasattr(embedding_responses, 'data') or not embedding_responses.data or 'values' not in embedding_responses.data[0]:
             raise ValueError("Failed to get embedding vector from Pinecone response.")
        
        query_embedding = embedding_responses.data[0]['values']

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