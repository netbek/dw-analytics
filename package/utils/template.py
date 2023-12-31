def render_template(file_path: str, context: dict):
    with open(file_path, "rt") as file:
        template = file.read()

    for variable, value in context.items():
        template = template.replace("{{ " + variable + " }}", value)

    with open(file_path, "wt") as file:
        file.write(template)
