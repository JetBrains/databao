import json

from portus.data_source.config_classes.schema_inspection_config import InspectionOptions
from portus.data_source.data_source import SemanticDict


def get_inspection_cache_key(semantic_dict: SemanticDict, inspection_options: InspectionOptions) -> str:
    key_dict = dict(semantic_dict=semantic_dict, inspection_options=inspection_options.model_dump_for_cache())
    key = json.dumps(key_dict, sort_keys=True)
    return key
