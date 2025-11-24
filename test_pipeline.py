import asyncio
import logging
from Miscellaneous import MongoSink, Chunk, VectorIndexConfig

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    # Connection string for the local docker instance
    connection_string = "mongodb+srv://ritikrajak:fncCz4n4addsOXhC@100x-dev-cluster.d2zhvta.mongodb.net/"
    db_name = "test_db"
    collection_name = "test_chunks"

    sink = MongoSink(connection_string, db_name)

    print("1. Connecting...")
    await sink.connect()

    print("2. Creating Collection...")
    await sink.create_collection(collection_name)

    print("3. Ingesting Chunks with Stable IDs (ignoring metadata in hash)...")
    
    text = "Stable Content"
    # Generate ID based ONLY on text
    chunk_id = Chunk.generate_id(text, include_metadata=False)
    print(f"   Generated ID: {chunk_id}")

    chunk = Chunk(
        id=chunk_id,
        text=text,
        embedding=[0.1, 0.2, 0.3],
        metadata={"version": 1, "status": "draft"}
    )
    
    await sink.ingest_chunks(collection_name, [chunk])

    print("4. Updating Metadata ONLY (Version 1 -> 2)...")
    new_metadata = {"version": 2, "status": "published"}
    
    await sink.update_chunk_metadata(collection_name, chunk_id, new_metadata)

    print("5. Verifying Update...")
    # Use get_chunk for immediate consistency check
    updated_chunk = await sink.get_chunk(collection_name, chunk_id)
    
    if updated_chunk:
        print(f"   Found Chunk. Metadata: {updated_chunk.metadata}")
        if updated_chunk.metadata.get("version") == 2:
            print("   SUCCESS: Metadata updated to version 2.")
        else:
            print("   FAILURE: Metadata not updated.")
    else:
        print("   FAILURE: Chunk not found after update.")

    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
