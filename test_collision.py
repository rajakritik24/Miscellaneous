import asyncio
import logging
from Miscellaneous import MongoSink, Chunk
from Miscellaneous.kb_manager import KnowledgeBaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    connection_string = ""
    db_name = "test_db"
    collection_name = "collision_test"

    sink = MongoSink(connection_string, db_name)
    await sink.connect()
    await sink.create_collection(collection_name)
    
    # Clear collection first
    await sink.db[collection_name].delete_many({})

    manager = KnowledgeBaseManager(sink, collection_name)

    # --- Test Collision ---
    print("\n--- Testing Collision ---")
    
    # Ingest File A
    print("1. Ingesting File A...")
    meta_a = {"filename": "file_a.pdf", "label": "A"}
    # Mock content will be "Content of file_a.pdf chunk 1" etc.
    # To force collision, we need to mock the content extraction to return SAME text
    
    # Monkey patch the mock extractor for this test
    original_extractor = manager._mock_extract_and_chunk
    manager._mock_extract_and_chunk = lambda path: ["Common Text Paragraph"]
    
    await manager.process_file("path/to/file_a.pdf", meta_a)
    
    # Ingest File B (Same content)
    print("2. Ingesting File B (Same Content)...")
    meta_b = {"filename": "file_b.pdf", "label": "B"}
    await manager.process_file("path/to/file_b.pdf", meta_b)
    
    # Restore extractor
    manager._mock_extract_and_chunk = original_extractor

    # Verify
    print("3. Verifying...")
    # We should have 2 chunks in total
    count = await sink.db[collection_name].count_documents({})
    print(f"   Total Chunks: {count}")
    
    if count == 2:
        print("   SUCCESS: Identical text from different files stored as separate chunks.")
        
        # Verify IDs are different
        cursor = sink.db[collection_name].find({})
        docs = await cursor.to_list(length=None)
        ids = [d["id"] for d in docs]
        print(f"   IDs: {ids}")
        assert ids[0] != ids[1]
        
    else:
        print(f"   FAILURE: Expected 2 chunks, found {count}. (Collision occurred)")

    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
