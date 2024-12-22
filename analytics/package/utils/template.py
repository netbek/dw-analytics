def render_template(file_path: str, context: dict):
    """
    TODO Consider replacing with Jinja if it's possible to preserve unescaped delimiters, e.g.
    {{ }} for expressions. This function is meant to render the given context only, not everything
    in the template.
    """
    with open(file_path, "rt") as file:
        template = file.read()

    for variable, value in context.items():
        template = template.replace("{{ " + variable + " }}", value)

    with open(file_path, "wt") as file:
        file.write(template)
