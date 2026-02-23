import pytest
import os
import sqlite3
import re
import shutil
from typing import List, Optional

from westronize.cli import init_db, apply_base_translation, ensure_xml_files

# Use relative paths from project root
EN_DB_PATH = "Texts_en_orig.db"
RU_DB_PATH = "texts_U46.1.0_v3.2.1.db"
OUTPUT_DB_NAME = "westronized.db"


def has_cyrillic(text: str) -> bool:
    """Checks if text contains Cyrillic characters."""
    return bool(re.search(r'[а-яА-ЯёЁ]', text))


def verify_hybrid_text(db_path: str, file_id: int, russian_ids: List[str]):
    """
    Verifies that for a given file_id:
    1. Text IDs in 'russian_ids' contain Cyrillic (Russian).
    2. All other Text IDs do NOT contain Cyrillic (English).
    """
    print(f"Checking File ID: {file_id}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT text_data FROM patch_data WHERE file_id=?", (file_id,))
    row = cursor.fetchone()
    
    assert row is not None, f"File ID {file_id} not found in output DB"
    text_data = row[0]
    conn.close()
    
    # Parse blob
    segments = text_data.split('|||')
    data = {}
    for seg in segments:
        if '::::::' in seg:
            parts = seg.split('::::::', 1)
            if len(parts) == 2:
                data[parts[0]] = parts[1]
    
    # Check for presence of at least one ID
    assert len(data) > 0, f"No text data found for file {file_id}"

    # Check Name IDs (English)
    # Any ID that is NOT in russian_ids is considered a Name part
    name_ids = [tid for tid in data.keys() if tid not in russian_ids]
    
    if not name_ids:
         print(f"  Warning: Only Russian IDs found for {file_id}. Skipping name check.")
    
    for name_id in name_ids:
        content = data[name_id]
        # Ignore empty strings or just numbers/symbols if any
        if not content.strip() or not re.search(r'[a-zA-Z]', content):
            continue
            
        assert not has_cyrillic(content), \
            f"Name ID {name_id} (File {file_id}) contains Cyrillic! Expected English. Content: {content}"

    # Check Russian IDs
    for ru_id in russian_ids:
        if ru_id in data:
            content = data[ru_id]
            # Heuristic: If it has letters and is long enough, it should have Cyrillic.
            if re.search(r'[a-zA-Z]', content) and len(content) > 20:
                 assert has_cyrillic(content), \
                    f"Russian ID {ru_id} (File {file_id}) seems to be English! Content: {content}"
        else:
            print(f"  Note: Russian ID {ru_id} not found/used for this file.")


@pytest.fixture(scope="module")
def db_builder(tmp_path_factory, request):
    """
    Factory fixture that returns a function to build/get the DB for a specific processor.
    Scope is module so we reuse the DB for all tests in that module.
    """
    
    # We use a cache to avoid rebuilding for the same processor in the same session if possible,
    # though scope="module" handles the per-module reuse.
    # This factory approach allows us to pass the Processor class.
    
    def _get_db(processor_class):
        if os.path.exists(OUTPUT_DB_NAME):
            print(f"\n[Setup] Using existing output DB: {OUTPUT_DB_NAME} (Skipping build)...")
            return OUTPUT_DB_NAME

        if not os.path.exists(EN_DB_PATH) or not os.path.exists(RU_DB_PATH):
            pytest.skip("Real database files not found. Skipping integration test.")

        # Create a unique DB for this processor type to avoid conflicts
        # (Though usually we run one test file at a time, or they are separate modules)
        proc_name = processor_class.__name__
        out_db = tmp_path_factory.mktemp(f"db_{proc_name}") / "test_out.db"
        
        print(f"\n[Setup] Initializing DB from {EN_DB_PATH}...")
        init_db(EN_DB_PATH, str(out_db))
        
        print(f"[Setup] Applying RU translation from {RU_DB_PATH}...")
        apply_base_translation(RU_DB_PATH, str(out_db))
        
        print(f"[Setup] Processing {proc_name} module...")
        processor = processor_class()
        
        # Ensure XML exists (using the new ensure_xml_files would be better but that's in wstrn.py main)
        # For tests, we assume wstrn.py logic or manual run. 
        # But wait, wstrn.py has ensure_xml_files. We should probably use it or Mock it.
        # For now, let's assume the user has run the tool once or files exist.
        # If not, the processor will skip or fail.
        # Let's import ensure_xml_files to be safe.
        from westronize.cli import ensure_xml_files
        ensure_xml_files()
        
        if not os.path.exists(processor.get_xml_path()):
             pytest.fail(f"XML not found for {proc_name}")

        processor.process(EN_DB_PATH, str(out_db), debug=True)
        
        return str(out_db)

    return _get_db
