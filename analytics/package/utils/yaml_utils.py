from pathlib import Path

import yaml


class PrettySafeDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

        # Add empty line between first level items
        if len(self.indents) == 1:
            super().write_line_break()


def safe_load_file(path: Path | str) -> dict:
    with open(path, "rt") as fp:
        data = yaml.safe_load(fp)

    return data
