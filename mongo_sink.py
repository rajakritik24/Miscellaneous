import logging
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pymongo.errors import PyMongoError, CollectionInvalid
from pymongo.operations import SearchIndexModel

from .base_sink import VectorSink
from .models import Chunk, VectorIndexConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoSink(VectorSink):
    """
    MongoDB Atlas implementation of the VectorSink using Motor for async I/O.
    """

    def __init__(self, connection_string: str, database_name: str):
        self.connection_string = connection_string
        self.database_name = database_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None

    async def connect(self):
        """Establish connection to MongoDB Atlas."""
        try:
            self.client = AsyncIOMotorClient(self.connection_string)
            self.db = self.client[self.database_name]
            # Ping to verify connection
            await self.client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB database: {self.database_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def create_collection(self, collection_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Create a collection if it does not exist.
        """
        if self.db is None:
            raise ConnectionError("Not connected to database. Call connect() first.")

        existing_collections = await self.db.list_collection_names()
        if collection_name in existing_collections:
            logger.info(f"Collection '{collection_name}' already exists.")
            return

        try:
            await self.db.create_collection(collection_name)
            logger.info(f"Collection '{collection_name}' created successfully.")
        except CollectionInvalid as e:
            logger.warning(f"Collection creation failed (might already exist): {e}")
        except Exception as e:
            logger.error(f"Error creating collection '{collection_name}': {e}")
            raise

    async def create_vector_index(self, collection_name: str, index_config: VectorIndexConfig):
        """
        Create a vector search index on the collection using the standard Atlas Vector Search definition.
        """
        if self.db is None:
            raise ConnectionError("Not connected to database.")

        collection = self.db[collection_name]
        
        definition = {
            "fields": [
                {
                    "type": "vector",
                    "path": index_config.path,
                    "numDimensions": index_config.dimensions,
                    "similarity": index_config.similarity
                }
            ]
        }

        model = SearchIndexModel(
            definition=definition,
            name=index_config.name,
            type="vectorSearch"
        )

        try:
            # Check if index already exists
            # Motor's list_search_indexes returns an async cursor
            cursor = collection.list_search_indexes(index_config.name)
            existing_indexes = await cursor.to_list(length=None)
            
            if existing_indexes:
                logger.info(f"Vector index '{index_config.name}' already exists on '{collection_name}'.")
                return

            await collection.create_search_index(model=model)
            logger.info(f"Vector index '{index_config.name}' creation initiated on '{collection_name}'.")
        except PyMongoError as e:
            logger.error(f"Failed to create vector index: {e}")
            raise

    async def validate_index(self, collection_name: str, index_name: str) -> bool:
        """
        Check if the search index exists.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")
        
        try:
            collection = self.db[collection_name]
            cursor = collection.list_search_indexes(index_name)
            indexes = await cursor.to_list(length=None)
            
            is_exists = len(indexes) > 0
            if is_exists:
                logger.info(f"Index '{index_name}' found on '{collection_name}'.")
            else:
                logger.warning(f"Index '{index_name}' NOT found on '{collection_name}'.")
            return is_exists
        except Exception as e:
            logger.error(f"Error validating index: {e}")
            return False

    async def ingest_chunks(self, collection_name: str, chunks: List[Chunk], batch_size: int = 100):
        """
        Ingest chunks using bulk write operations for efficiency.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")

        collection = self.db[collection_name]
        
        total_chunks = len(chunks)
        logger.info(f"Starting ingestion of {total_chunks} chunks into '{collection_name}'...")

        for i in range(0, total_chunks, batch_size):
            batch = chunks[i : i + batch_size]
            operations = []
            
            for chunk in batch:
                doc = chunk.model_dump()
                operations.append(
                    UpdateOne(
                        {"id": chunk.id},
                        {"$set": doc},
                        upsert=True
                    )
                )
            
            if operations:
                try:
                    result = await collection.bulk_write(operations)
                    logger.info(f"Batch {i//batch_size + 1}: Matched {result.matched_count}, Modified {result.modified_count}, Upserted {result.upserted_count}")
                except PyMongoError as e:
                    logger.error(f"Error ingesting batch starting at index {i}: {e}")
                    raise

        logger.info("Ingestion complete.")

    async def delete_chunks(self, collection_name: str, filters: Dict[str, Any]):
        """
        Delete chunks based on a filter.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")

        collection = self.db[collection_name]
        
        try:
            result = await collection.delete_many(filters)
            logger.info(f"Deleted {result.deleted_count} chunks from '{collection_name}' matching filter: {filters}")
        except PyMongoError as e:
            logger.error(f"Error deleting chunks: {e}")
            raise

    async def update_chunk_metadata(self, collection_name: str, chunk_id: str, metadata: Dict[str, Any]):
        """
        Update the metadata of a specific chunk without re-ingesting the embedding.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")

        collection = self.db[collection_name]
        
        try:
            result = await collection.update_one(
                {"id": chunk_id},
                {"$set": {"metadata": metadata}}
            )
            if result.matched_count == 0:
                logger.warning(f"Chunk with id '{chunk_id}' not found. Metadata update skipped.")
            else:
                logger.info(f"Updated metadata for chunk '{chunk_id}'.")
        except PyMongoError as e:
            logger.error(f"Error updating chunk metadata: {e}")
            raise

    async def get_chunk(self, collection_name: str, chunk_id: str) -> Optional[Chunk]:
        """
        Retrieve a single chunk by ID.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")
        
        collection = self.db[collection_name]
        try:
            doc = await collection.find_one({"id": chunk_id})
            if doc:
                if "_id" in doc:
                    del doc["_id"]
                return Chunk(**doc)
            return None
        except PyMongoError as e:
            logger.error(f"Error retrieving chunk: {e}")
            raise

    async def get_file_info(self, collection_name: str, filename: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specific file.
        We query for ONE chunk that matches the filename in metadata.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")
        
        collection = self.db[collection_name]
        try:
            # Find one document where metadata.filename matches
            doc = await collection.find_one(
                {"metadata.filename": filename},
                {"metadata": 1, "_id": 0} # Projection: only return metadata
            )
            if doc and "metadata" in doc:
                return doc["metadata"]
            return None
        except PyMongoError as e:
            logger.error(f"Error retrieving file info: {e}")
            raise

    async def create_metadata_index(self, collection_name: str, field: str = "metadata.filename"):
        """
        Create a standard database index on a metadata field.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")
        
        collection = self.db[collection_name]
        try:
            await collection.create_index([(field, 1)])
            logger.info(f"Created index on '{field}' in '{collection_name}'.")
        except PyMongoError as e:
            logger.error(f"Error creating metadata index: {e}")
            raise

    async def search(self, collection_name: str, query_vector: List[float], limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Chunk]:
        """
        Perform a vector search using the $vectorSearch aggregation stage.
        """
        if self.db is None:
            raise ConnectionError("Not connected.")
            
        collection = self.db[collection_name]
        
        index_name = "vector_index" 
        path = "embedding"

        pipeline = [
            {
                "$vectorSearch": {
                    "index": index_name,
                    "path": path,
                    "queryVector": query_vector,
                    "numCandidates": limit * 10,
                    "limit": limit
                }
            }
        ]

        if filters:
            pipeline[0]["$vectorSearch"]["filter"] = filters

        pipeline.append({
            "$project": {
                "_id": 0,
                "id": 1,
                "text": 1,
                "embedding": 1,
                "metadata": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        })

        try:
            # Motor's aggregate returns an async cursor
            cursor = collection.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            chunks = [Chunk(**r) for r in results]
            return chunks
        except PyMongoError as e:
            logger.error(f"Search failed: {e}")
            raise
