import datetime
import json
import os


def save_file(filename, content):
    with open(filename, 'w') as f:
        f.write(content)


def read_file(filename):
    with open(filename, 'r') as f:
        return f.read()


def save_last_data_timestamp(now):
    save_file('last_data.txt', now.replace(microsecond=0).isoformat())


def get_last_data_timestamp():
    return datetime.datetime.fromisoformat(read_file('last_data.txt'))


def save_last_config_timestamp(now):
    save_file('last_config.txt', now.replace(microsecond=0).isoformat())


def get_last_config_timestamp():
    return datetime.datetime.fromisoformat(read_file('last_config.txt'))


def load_config_versions(local_root: str) -> dict[str, dict[str, dict]]:
    """Load downloaded config JSONs into a nested dict.

    Expects the following local structure:

        local_root/
            <config_name_1>/
                <version_1>.json
                <version_2>.json
                ...
            <config_name_2>/
                <version_1>.json
                ...

    Returns:
        dict mapping config_name -> dict[version -> JSON content as dict].
    """
    configs: dict[str, dict[str, dict]] = {}

    if not os.path.isdir(local_root):
        return configs

    for entry in os.scandir(local_root):
        if not entry.is_dir():
            continue
        config_name = entry.name
        versions: dict[str, dict] = {}

        for file_entry in os.scandir(entry.path):
            if not file_entry.is_file():
                continue
            if not file_entry.name.lower().endswith('.json'):
                continue
            version = file_entry.name[:-5]  # strip .json
            try:
                with open(file_entry.path, 'r', encoding='utf-8') as f:
                    versions[version] = json.load(f)
            except json.JSONDecodeError:
                # Skip invalid JSON files rather than failing the whole load.
                continue

        if versions:
            configs[config_name] = versions

    return configs
