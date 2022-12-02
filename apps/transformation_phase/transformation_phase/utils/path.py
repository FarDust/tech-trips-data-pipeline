from re import compile, search

from transformation_phase.constants.columns import Columns


def get_region_from_path(path: str) -> str:
    region_regex = compile(rf"{Columns.REGION}=(\w+)\/")
    matched_str = search(region_regex, path)
    return matched_str.group(1) if matched_str else ""
