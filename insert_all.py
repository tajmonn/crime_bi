from crime import main as crime_main
from czas import main as czas_main
from data import main as data_main
from opis import main as opis_main
from pogoda import main as pogoda_main
from populacja import main as populacja_main
from przychod import main as przychod_main
from sasiedztwo import main as sasiedztwo_main
from typ import main as typ_main


def execute_functions_in_order() -> None:
    functions = [
        ("czas_main", czas_main),
        ("data_main", data_main),
        ("opis_main", opis_main),
        ("pogoda_main", pogoda_main),
        ("populacja_main", populacja_main),
        ("przychod_main", przychod_main),
        ("sasiedztwo_main", sasiedztwo_main),
        ("typ_main", typ_main),
        ("crime_main", crime_main),
    ]

    for name, function in functions:
        print(f"Executing function: {name}")
        function()


if __name__ == "__main__":
    execute_functions_in_order()
