import sqlite3
import logging
import os
import xml.etree.ElementTree as ET
from typing import List, Dict, Set, Tuple

logger = logging.getLogger(__name__)

class BaseProcessor:
    def __init__(self, name: str):
        self.name = name

    def get_xml_path(self) -> str:
        """Returns the path to the XML file for this module."""
        raise NotImplementedError

    def get_excluded_ids(self) -> Set[str]:
        """Returns a set of Text IDs to exclude from reversion (e.g. descriptions)."""
        return set()

    def get_xml_tag(self) -> str:
        """Returns the XML tag to search for (e.g., 'skill', './/label')."""
        return ".//label"  # Default for Items

    def get_key_attributes(self) -> List[str]:
        """Returns a list of attributes that contain the 'key:FILE_ID:...' string."""
        return ["key"]  # Default for Items

    def process(self, en_db_path: str, target_db_path: str, debug: bool = False):
        """Main processing logic for the module."""
        xml_path = self.get_xml_path()
        if not os.path.exists(xml_path):
            logger.warning(f"XML file not found for module '{self.name}': {xml_path}. Skipping.")
            return

        mapping = self._parse_xml_mapping(xml_path)
        if not mapping:
            logger.info(f"No valid mappings found for module '{self.name}'.")
            return

        self._revert_names(en_db_path, target_db_path, mapping, debug)

    def _parse_xml_mapping(self, xml_path: str) -> Dict[int, bool]:
        """
        Parses XML to find which files need reversion.
        Returns a dict: {file_id: True}
        """
        logger.info(f"[{self.name}] Parsing XML mapping from '{xml_path}'...")
        mapping = {}
        
        target_tag = self.get_xml_tag()
        target_attrs = self.get_key_attributes()
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            elements = root.findall(target_tag)
            logger.info(f"[{self.name}] Found {len(elements)} elements ('{target_tag}') to check.")
            
            count = 0
            for elem in elements:
                for attr in target_attrs:
                    val = elem.get(attr)
                    if not val or not val.startswith("key:"):
                        continue
                        
                    parts = val.split(':')
                    if len(parts) < 3:
                        continue
                    
                    try:
                        file_id = int(parts[1])
                        if file_id not in mapping:
                            mapping[file_id] = True
                            count += 1
                    except ValueError:
                        continue
                
            logger.info(f"[{self.name}] Found {count} files to process.")
            return mapping
        except Exception as e:
            logger.error(f"[{self.name}] Error parsing XML: {e}")
            raise

    def _revert_names(self, en_db_path: str, target_db_path: str, mapping: Dict[int, bool], debug: bool = False):
        logger.info(f"[{self.name}] Reverting ALL names (except excluded IDs) to English...")
        
        conn_en = sqlite3.connect(en_db_path)
        cursor_en = conn_en.cursor()
        
        conn_out = sqlite3.connect(target_db_path)
        cursor_out = conn_out.cursor()
        
        excluded_ids = self.get_excluded_ids()
        updates = []
        
        def parse_blob(blob: str) -> Dict[str, str]:
            segments = blob.split('|||')
            seg_map = {}
            for seg in segments:
                if '::::::' in seg:
                    parts = seg.split('::::::', 1)
                    if len(parts) == 2:
                        tid, content = parts
                        seg_map[tid] = content
            return seg_map

        # Reversion loop
        logger.info(f"[{self.name}] Processing {len(mapping)} files...")
        for file_id in mapping.keys():
            cursor_en.execute("SELECT text_data FROM patch_data WHERE file_id = ?", (file_id,))
            row_en = cursor_en.fetchone()
            if not row_en or not row_en[0]:
                continue
            en_text_blob = row_en[0]
            
            cursor_out.execute("SELECT text_data FROM patch_data WHERE file_id = ?", (file_id,))
            row_out = cursor_out.fetchone()
            if not row_out or not row_out[0]:
                continue
            out_text_blob = row_out[0]
            
            en_segments = parse_blob(en_text_blob)
            
            modified = False
            
            new_segments = []
            raw_segments = out_text_blob.split('|||')
            
            reverted_items = []
            
            for seg in raw_segments:
                if '::::::' in seg:
                    parts = seg.split('::::::', 1)
                    if len(parts) == 2:
                        tid, content = parts
                        
                        # LOGIC CHANGE: 
                        # If ID is NOT in excluded list (Description), AND exists in English DB, REVERT IT.
                        if tid not in excluded_ids and tid in en_segments:
                            new_segments.append(f"{tid}::::::" + en_segments[tid])
                            modified = True
                            if debug:
                                reverted_items.append(tid)
                        else:
                            # Keep current (Russian) content (e.g. Description or missing in EN)
                            new_segments.append(seg)
                    else:
                        new_segments.append(seg)
                else:
                    new_segments.append(seg)
            
            if modified:
                new_blob = "|||".join(new_segments)
                updates.append((new_blob, file_id))
                if debug and reverted_items:
                    logger.debug(f"[{self.name}] Reverted file_id={file_id} text_ids={','.join(reverted_items)}")


            
        if updates:
            logger.info(f"[{self.name}] Committing {len(updates)} reverted text blobs...")
            # Batch update might be slow if list is huge, can batch in chunks if needed
            # For 10k items it should be fine.
            cursor_out.executemany("UPDATE patch_data SET text_data = ? WHERE file_id = ?", updates)
            conn_out.commit()
            
        conn_en.close()
        conn_out.close()
        logger.info(f"[{self.name}] Reversion complete.")

