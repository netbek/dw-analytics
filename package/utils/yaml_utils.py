import yaml


class PrettySafeDumper(yaml.SafeDumper):
    def write_line_break(self, data=None):
        super().write_line_break(data)

        # Add empty line between first level items
        if len(self.indents) == 1:
            super().write_line_break()
