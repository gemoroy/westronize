import pytest
import sqlite3
import os
import re
from tests.conftest import verify_hybrid_text
from westronize.modules.skills import Processor as SkillsProcessor

@pytest.fixture(scope="module")
def skills_db(db_builder):
    """
    Module-scoped fixture to build/get the DB for Skills.
    """
    return db_builder(SkillsProcessor)

@pytest.mark.parametrize("file_id", [620759186, 620759291, 620759440, 620759617, 620764915])
def test_skills_processing(skills_db, file_id):
    """
    Tests specific skill IDs to verify:
    1. Names (non-desc IDs) do NOT contain Cyrillic (should be English).
    2. Descriptions (desc IDs) DO contain Cyrillic (should be Russian).
    """
    # Description ID for skills
    DESC_ID = "228830419"
    
    verify_hybrid_text(skills_db, file_id, russian_ids=[DESC_ID])
