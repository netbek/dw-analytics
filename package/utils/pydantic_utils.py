import csv
import io
import json

LINE_TERMINATOR = "\n"


def dump_csv(*models) -> str:
    """Serialize the model(s) to a CSV string."""
    if not models:
        return ""

    data = [model.model_dump() for model in models]

    output = io.StringIO()
    writer = csv.writer(output, lineterminator=LINE_TERMINATOR)
    headers = data[0].keys()
    writer.writerow(headers)

    for entry in data:
        row = []
        for key in headers:
            value = entry[key]
            if value is None:
                row.append("null")
            elif isinstance(value, (dict, list)):
                # TODO Improve null handling. None (NoneType) must be replaced with null,
                # but "None" (string) must be kept as is, e.g
                # Input: ["None", None]
                # Output: "[""None"", null]"
                row.append(json.dumps(value, default=str).replace("None", "null"))
            else:
                row.append(value)
        writer.writerow(row)

    value = output.getvalue().strip()

    if value == "":
        return value
    else:
        return value + LINE_TERMINATOR
