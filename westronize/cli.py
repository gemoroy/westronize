import argparse
import sqlite3
import shutil
import logging
import os
import re
import requests
from typing import List, Dict

from .modules.items import Processor as ItemsProcessor
from .modules.skills import Processor as SkillsProcessor
from .modules.traits import Processor as TraitsProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Registry of available processors
PROCESSORS = {
    'items': ItemsProcessor,
    'skills': SkillsProcessor,
    'traits': TraitsProcessor
}

XML_SOURCES = {
    "lore/labels/en/items.xml": "https://raw.githubusercontent.com/LotroCompanion/lotro-data/refs/heads/master/lore/labels/en/items.xml",
    "lore/skills.xml": "https://raw.githubusercontent.com/LotroCompanion/lotro-data/refs/heads/master/lore/skills.xml",
    "lore/traits.xml": "https://raw.githubusercontent.com/LotroCompanion/lotro-data/refs/heads/master/lore/traits.xml"
}

def ensure_xml_files():
    """
    Checks for required XML files. Downloads them if missing or outdated.
    Uses ETag for caching.
    """
    logger.info("Checking XML data files...")
    
    for local_path, url in XML_SOURCES.items():
        etag_path = f"{local_path}.etag"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        headers = {}
        if os.path.exists(local_path) and os.path.exists(etag_path):
            with open(etag_path, 'r') as f:
                etag = f.read().strip()
                if etag:
                    headers['If-None-Match'] = etag
        
        try:
            logger.info(f"Checking {local_path}...")
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code == 304:
                logger.info(f"  -> Up to date (Cached).")
                continue
                
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                size_mb = total_size / (1024 * 1024)
                logger.info(f"  -> Downloading new version ({size_mb:.2f} MB)...")
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                
                logger.info(f"  -> Download complete.")
                
                # Save ETag
                new_etag = response.headers.get('ETag')
                if new_etag:
                    with open(etag_path, 'w') as f:
                        f.write(new_etag)
                
            else:
                logger.warning(f"  -> Failed to check/download {url}. Status: {response.status_code}")
                
        except requests.RequestException as e:
            if os.path.exists(local_path):
                logger.warning(f"  -> Network error checking {local_path}: {e}. Using local copy.")
            else:
                logger.error(f"  -> Failed to download {local_path}: {e}")
                raise

def init_db(en_db_path: str, output_db_path: str):
    """Creates a fresh copy of the English DB as the output DB."""
    logger.info(f"Creating output database '{output_db_path}' from '{en_db_path}'...")
    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    shutil.copy2(en_db_path, output_db_path)
    logger.info("Output database created.")

def apply_base_translation(ru_db_path: str, output_db_path: str):
    """Applies the full Russian translation to the output DB using SQLite ATTACH."""
    logger.info("Applying base Russian translation...")
    
    conn = sqlite3.connect(output_db_path)
    cursor = conn.cursor()
    
    try:
        # Attach the RU database
        cursor.execute(f"ATTACH DATABASE '{ru_db_path}' AS ru_db")
        
        logger.info("Building file_id mapping from RU database...")
        cursor.execute("SELECT rowid, options FROM ru_db.patch_data WHERE options LIKE '%fid:%'")
        ru_rows = cursor.fetchall()
        
        ru_map = {} # file_id -> ru_rowid
        fid_pattern = re.compile(r'fid:\s*(\d+)')
        
        logger.info(f"Processing {len(ru_rows)} records...")
        for rowid, options in ru_rows:
            if not options:
                continue
            match = fid_pattern.search(options)
            if match:
                try:
                    fid = int(match.group(1))
                    ru_map[fid] = rowid
                except ValueError:
                    continue
                
        logger.info(f"Found {len(ru_map)} matching records in RU database.")
        
        # Now update efficiently
        cursor.execute("CREATE TEMPORARY TABLE ru_mapping (file_id INTEGER PRIMARY KEY, ru_rowid INTEGER)")
        cursor.executemany("INSERT INTO ru_mapping (file_id, ru_rowid) VALUES (?, ?)", ru_map.items())
        
        logger.info("Updating text_data in output database...")
        # Since this is a single giant update, executemany is fast enough.
        cursor.execute("""
            UPDATE patch_data
            SET text_data = (
                SELECT text_data 
                FROM ru_db.patch_data 
                WHERE ru_db.patch_data.rowid = (
                    SELECT ru_rowid FROM ru_mapping WHERE ru_mapping.file_id = patch_data.file_id
                )
            )
            WHERE file_id IN (SELECT file_id FROM ru_mapping)
        """)
        
        conn.commit()
        logger.info(f"Updated {cursor.rowcount} rows with Russian text.")
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="LOTRO Text Database Patcher (Modular)")
    parser.add_argument("--en-db", default="Texts_en_orig.db", help="Path to original English DB (default: Texts_en_orig.db)")
    parser.add_argument("--ru-db", required=True, help="Path to Russian DB")
    parser.add_argument("--output-db", default="westronized.db", help="Path to output DB (default: westronized.db)")
    
    available_blocks = ", ".join(PROCESSORS.keys())
    parser.add_argument("--blocks", default="items,skills,traits", help=f"Comma-separated list of content blocks to process. Available: {available_blocks} (default: items,skills,traits)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging to file (wstrn_debug.log)")
    
    args = parser.parse_args()
    
    if args.debug:
        file_handler = logging.FileHandler('wstrn_debug.log', mode='w')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled. Writing to wstrn_debug.log")
    
    if not os.path.exists(args.en_db):
        logger.error(f"English DB not found at: {args.en_db}")
        logger.error("Please ensure 'Texts_en_orig.db' exists in the current directory or provide the path using --en-db")
        return
    if not os.path.exists(args.ru_db):
        logger.error(f"Russian DB not found: {args.ru_db}")
        return

    try:
        # Step 0: Ensure XML files are present and up-to-date
        ensure_xml_files()

        # Step 1: Init Output DB (Shared Base)
        init_db(args.en_db, args.output_db)
        
        # Step 2: Apply Base Translation (Shared Base)
        apply_base_translation(args.ru_db, args.output_db)
        
        # Step 3: Apply Modules
        blocks = [b.strip().lower() for b in args.blocks.split(',')]
        
        for block in blocks:
            if block in PROCESSORS:
                logger.info(f"Starting processing for block: {block}")
                processor = PROCESSORS[block]()
                processor.process(args.en_db, args.output_db, debug=args.debug)
            else:
                logger.warning(f"Unknown block '{block}'. Available blocks: {', '.join(PROCESSORS.keys())}")
        
        logger.info("All requested patching completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
