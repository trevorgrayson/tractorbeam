from apache_beam import Map


example_pipeline = (
    "arrows" >> Map(lambda x: f">{x}<")
    | "equals" >> Map(lambda x: f"{x}===")
)
