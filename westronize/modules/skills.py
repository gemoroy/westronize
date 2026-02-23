from .base import BaseProcessor
from typing import List

class Processor(BaseProcessor):
    def __init__(self):
        super().__init__("Skills")
        # Skills description ID based on XML analysis (228830419)
        self.desc_id = "228830419"

    def get_xml_path(self) -> str:
        return "lore/skills.xml"

    def get_excluded_ids(self):
        return {self.desc_id}

    def get_xml_tag(self) -> str:
        return "skill"

    def get_key_attributes(self) -> List[str]:
        return ["description"]
