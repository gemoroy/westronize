from .base import BaseProcessor
from typing import List, Set

class Processor(BaseProcessor):
    def __init__(self):
        super().__init__("Traits")
        # Traits description ID: 54354734 (Same as Items, surprisingly!)
        self.desc_id = "54354734"
        # Tooltip ID: 191029568
        self.tooltip_id = "191029568"

    def get_xml_path(self) -> str:
        return "lore/traits.xml"

    def get_excluded_ids(self) -> Set[str]:
        # Keep both Description and Tooltip as Russian
        return {self.desc_id, self.tooltip_id}

    def get_xml_tag(self) -> str:
        return "trait"

    def get_key_attributes(self) -> List[str]:
        return ["description", "tooltip"]
