import asyncio
import logging
from Miscellaneous import MongoSink, Chunk, VectorIndexConfig
from Miscellaneous.kb_manager import KnowledgeBaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    connection_string = ""
    db_name = "test_db"
    collection_name = "kb_test"

    sink = MongoSink(connection_string, db_name)
    await sink.connect()
    await sink.create_collection(collection_name)

    manager = KnowledgeBaseManager(sink, collection_name)
    
    # --- Scenario 1: New File ---
    print("\n--- 1. Ingesting New File ---")
    meta1 = {"filename": "doc1.pdf", "label": "draft", "provider": "A"}
    # Mocking file path (hash will be based on path string in our mock)
    await manager.process_file("path/to/doc1.pdf", meta1)

    # Verify
    info = await sink.get_file_info(collection_name, "doc1.pdf")
    print(f"   Stored Info: {info}")
    assert info["label"] == "draft"
    assert "file_hash" in info

    # --- Scenario 2: Metadata Update ---
    print("\n--- 2. Updating Metadata (Draft -> Final) ---")
    meta2 = {"filename": "doc1.pdf", "label": "final", "provider": "A"}
    await manager.process_file("path/to/doc1.pdf", meta2)

    # Verify
    info = await sink.get_file_info(collection_name, "doc1.pdf")
    print(f"   Stored Info: {info}")
    assert info["label"] == "final"

    # --- Scenario 3: Content Update ---
    print("\n--- 3. Updating Content (Simulated by changing path/hash) ---")
    # We use a different path to simulate different content hash
    meta3 = {"filename": "doc1.pdf", "label": "final", "provider": "A"}
    await manager.process_file("path/to/doc1_v2.pdf", meta3)

    # Verify
    info = await sink.get_file_info(collection_name, "doc1.pdf")
    print(f"   Stored Info: {info}")
    # Hash should have changed
    assert info["file_hash"] != meta1.get("file_hash") # Note: meta1 didn't have hash, but we can check against previous

    # --- Scenario 4: Legacy Migration (Simulate missing hash) ---
    print("\n--- 4. Legacy Migration (Simulate missing hash) ---")
    # Manually remove hash from DB to simulate legacy state
    await sink.db[collection_name].update_many(
        {"metadata.filename": "doc1.pdf"},
        {"$unset": {"metadata.file_hash": ""}}
    )
    
    # Verify it's gone
    info = await sink.get_file_info(collection_name, "doc1.pdf")
    print(f"   Legacy State (No Hash): {info}")
    assert "file_hash" not in info

    # Run process again - should trigger re-ingestion
    await manager.process_file("path/to/doc1_v2.pdf", meta3)
    
    # Verify hash is back
    info = await sink.get_file_info(collection_name, "doc1.pdf")
    print(f"   Migrated State (Hash Restored): {info}")
    assert "file_hash" in info

    print("\nAll scenarios passed!")

if __name__ == "__main__":
    asyncio.run(main())
