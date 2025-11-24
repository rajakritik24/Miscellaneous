from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from .models import Chunk, VectorIndexConfig

class VectorSink(ABC):
    """
    Abstract base class for a Vector Database Sink.
    Defines the contract for ingesting, deleting, and managing vector data.
    """

    @abstractmethod
    async def connect(self):
        """Establish connection to the database."""
        pass

    @abstractmethod
    async def create_collection(self, collection_name: str, config: Optional[Dict[str, Any]] = None):
        """Create a collection/table if it does not exist."""
        pass

    @abstractmethod
    async def create_vector_index(self, collection_name: str, index_config: VectorIndexConfig):
        """Create a vector index on the collection."""
        pass

    @abstractmethod
    async def validate_index(self, collection_name: str, index_name: str) -> bool:
        """Check if a specific index exists and is ready."""
        pass

    @abstractmethod
    async def ingest_chunks(self, collection_name: str, chunks: List[Chunk]):
        """
        Ingest a list of chunks into the database. 
        Should handle upserts (update if exists, insert if new).
        """
        pass

    @abstractmethod
    async def delete_chunks(self, collection_name: str, filters: Dict[str, Any]):
        """
        Delete chunks matching the specific filters.
        """
        pass

    @abstractmethod
    async def update_chunk_metadata(self, collection_name: str, chunk_id: str, metadata: Dict[str, Any]):
        """
        Update the metadata of a specific chunk without re-ingesting the embedding.
        """
        pass

    @abstractmethod
    async def get_chunk(self, collection_name: str, chunk_id: str) -> Optional[Chunk]:
        """
        Retrieve a single chunk by ID.
        """
        pass

    @abstractmethod
    async def get_file_info(self, collection_name: str, filename: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specific file (e.g. to check existence or hash).
        Returns the metadata of the first chunk found for this file.
        """
        pass

    @abstractmethod
    async def create_metadata_index(self, collection_name: str, field: str = "metadata.filename"):
        """
        Create a standard database index on a metadata field to optimize lookups.
        """
        pass
    
    @abstractmethod
    async def search(self, collection_name: str, query_vector: List[float], limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Perform a vector search (useful for verification/testing).
        """
        pass
