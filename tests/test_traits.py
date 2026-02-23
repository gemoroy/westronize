import pytest
import sqlite3
import os
import re
from tests.conftest import verify_hybrid_text
from westronize.modules.traits import Processor as TraitsProcessor

@pytest.fixture(scope="module")
def traits_db(db_builder):
    """
    Module-scoped fixture to build/get the DB for Traits.
    """
    return db_builder(TraitsProcessor)

@pytest.mark.parametrize("file_id", [620757613, 620758784, 620761954])
def test_traits_processing(traits_db, file_id):
    """
    Tests specific trait IDs to verify:
    1. Names (non-desc/tooltip IDs) do NOT contain Cyrillic (should be English).
    2. Descriptions (desc IDs) DO contain Cyrillic (should be Russian).
    3. Tooltips (tooltip IDs) DO contain Cyrillic (should be Russian).
    """
    DESC_ID = "54354734"
    TOOLTIP_ID = "191029568"
    
    verify_hybrid_text(traits_db, file_id, russian_ids=[DESC_ID, TOOLTIP_ID])
