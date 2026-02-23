import pytest
import sqlite3
import os
import re
from tests.conftest import verify_hybrid_text
from westronize.modules.items import Processor as ItemsProcessor

@pytest.fixture(scope="module")
def items_db(db_builder):
    """
    Module-scoped fixture to build/get the DB for Items.
    """
    return db_builder(ItemsProcessor)

@pytest.mark.parametrize("file_id", [620879484, 621090749, 621087990, 620926092, 620953225])
def test_items_processing(items_db, file_id):
    """
    Tests specific item IDs to verify:
    1. Names (non-desc IDs) do NOT contain Cyrillic (should be English).
    2. Descriptions (desc IDs) DO contain Cyrillic (should be Russian).
    """
    # Description ID for items
    DESC_ID = "54354734"
    
    verify_hybrid_text(items_db, file_id, russian_ids=[DESC_ID])
