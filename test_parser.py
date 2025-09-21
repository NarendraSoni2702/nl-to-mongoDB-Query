import json
from parser import parse_aggregation_query
from schema import SCHEMA
from rich import print
from rich.console import Console
from deepdiff import DeepDiff

console = Console()

TEST_CASES = [
    (
        "sales where year > 2020 and region is north unwind products grouped by region only show region, sales if sales > 1000 then 'high' else 'low'",
        {
            "collection": "sales",
            "pipeline": [
                {"$match": {"$and": [{"year": {"$gt": 2020}}, {"region": "north"}]}},
                {"$unwind": "$products"},
                {"$group": {"_id": {"region": "$region"}, "total_sales": {"$sum": "$sales"}}},
                {"$project": {"region": 1, "sales": 1}},
                {"$project": {
                    "sales_category": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gt": ["$sales", 1000]}, "then": "high"}
                            ],
                            "default": "low"
                        }
                    }
                }}
            ]
        }
    ),
]

def run_test_case(nl_query, expected):
    result = parse_aggregation_query(nl_query, schema=SCHEMA)
    diff = DeepDiff(result, expected, ignore_order=True)
    if diff:
        console.print(f"[bold red]FAIL[/bold red]:\nNL Query: {nl_query}\n")
        console.print("[yellow]Expected:[/yellow]")
        console.print_json(json.dumps(expected, indent=2))
        console.print("[yellow]Got:[/yellow]")
        console.print_json(json.dumps(result, indent=2))
        console.print("[red]Differences:[/red]")
        console.print(diff.pretty())
    else:
        console.print(f"[bold green]PASS[/bold green]: {nl_query}")

def main():
    for nl_query, expected in TEST_CASES:
        run_test_case(nl_query, expected)

if __name__ == "__main__":
    main()
