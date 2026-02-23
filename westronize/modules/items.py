from .base import BaseProcessor
import os

class Processor(BaseProcessor):
    def __init__(self):
        super().__init__("Items")

    def get_xml_path(self) -> str:
        # Assuming the script runs from root 'wstrn' directory
        return "lore/labels/en/items.xml"

    def get_excluded_ids(self):
        return {"54354734"} # Items Description ID
