# petabase

Command line tool to mass manage Metatabase questions and dashboards.

## Setup

```bash
pipenv install
```

## Use

Activate the Python environment

```text
pipenv shell
```

Clone a collection of cards (simple copy):

```bash
python3 petabase.py --clone [id of source collection] [id of parent collection of the clone] 
```

Clone a collection of cards and dashboards, and update the name of the cards according to the name of the new collection:

Initial situation:

```text
Production (collection id = 1)
    -> BSDD (collection id = 12)
      -> Card 1 (BSDD)
      -> Card 2 (BSDD)
```

```bash
python3 petabase.py --clone 12 1 --bsdType DASRI
```

New situation:

```text
Production (collection id = 1)
    -> DASRI (collection id = 13)
      -> Card 1 (DASRI)
      -> Card 2 (DASRI)
    -> BSDD (collection id = 12)
      -> Card 1 (BSDD)
      -> Card 2 (BSDD)
```

Set the name of the cards (not dashboards) of a collection according to the name of the collection:

Initial situation:

```text
Production (collection id = 1)
    -> BSDD (collection id = 12)
      -> Card 1
      -> Card 2 (ABCD)
```

```bash
python3 petabase.py --setNames 12
```

New situation:

```text
Production (collection id = 1)
    -> BSDD (collection id = 12)
      -> Card 1 (BSDD)
      -> Card 2 (BSDD)
```


