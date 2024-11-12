#!/opt/venv/bin/python
from pathlib import Path

import os
import subprocess
import sys

SQLFLUFF_WORKING_DIRECTORY = "/"  # VS Code setting: `sqlfluff.workingDirectory`


def find_up(path, pattern):
    path = Path(path)

    if path.is_file():
        return find_up(path.parent, pattern)

    matches = list(path.glob(pattern))

    if matches:
        return matches[0]

    if path == path.parent:
        return None

    return find_up(path.parent, pattern)


def main(args):
    # If the script is called without arguments
    if len(args) < 2:
        return

    command = args[1]

    cmd = ["sqlfluff"] + args[1:]
    cwd = SQLFLUFF_WORKING_DIRECTORY

    if len(args) >= 3:
        sql_path = Path(os.path.join(SQLFLUFF_WORKING_DIRECTORY, args[-1:][0]))

        if sql_path.is_file():
            # Find the nearest dbt_project.yml to the SQL file
            match = find_up(sql_path, "dbt_project.yml")
            if match:
                cmd = ["sqlfluff"] + args[1:-1] + [str(sql_path)]
                # Run sqlfluff from the dbt_project.yml parent directory (dbt)
                cwd = match.parent

    try:
        output = subprocess.check_output(cmd, cwd=cwd).decode().strip()
        print(output)
    except subprocess.CalledProcessError as exc:
        if command == "lint":
            # sqlfluff raises an error if the SQL file has violations
            output = exc.output.decode().strip()
            print(output)
        else:
            raise exc


if __name__ == "__main__":
    main(sys.argv)
