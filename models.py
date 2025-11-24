from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from enum import Enum

class Chunk(BaseModel):
    """
    Represents a single chunk of data to be ingested.
    """
    id: str = Field(..., description="Unique identifier for the chunk")
    text: str = Field(..., description="The text content of the chunk")
    embedding: List[float] = Field(..., description="The vector embedding of the text")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata associated with the chunk")

    @staticmethod
    def generate_id(text: str, metadata: Dict[str, Any] = None, include_metadata: bool = False, salt: str = "") -> str:
        """
        Generates a deterministic ID based on the text and optionally metadata/salt.
        
        Args:
            text: The text content to hash.
            metadata: The metadata dictionary.
            include_metadata: If True, metadata is included in the hash.
            salt: Optional string to ensure uniqueness (e.g., filename). 
                  Use this to prevent identical text in different files from colliding.
        """
        import hashlib
        import json
        
        content_to_hash = text
        
        if salt:
            content_to_hash += f"|{salt}"
        
        if include_metadata and metadata:
            # We sort keys in metadata to ensure {"a": 1, "b": 2} produces same hash as {"b": 2, "a": 1}
            meta_str = json.dumps(metadata, sort_keys=True)
            content_to_hash += f"|{meta_str}"
        
        return hashlib.md5(content_to_hash.encode('utf-8')).hexdigest()

class IndexType(str, Enum):
    VECTOR = "vector"
    # Add other types if needed in future

class VectorIndexConfig(BaseModel):
    """
    Configuration for a vector index.
    """
    name: str
    dimensions: int
    path: str
    similarity: str = "cosine"  # cosine, euclidean, dotProduct
    
    # Additional Atlas specific configs can be added here
    num_candidates: int = 100 # For EF construction
