import argparse
from metabase_api import Metabase_API
import os
import re

mb_url = os.getenv('METABASE_URL')
mb_user = os.getenv('METABASE_USER')
mb_password = os.getenv('METABASE_PASSWORD')

try:
    assert isinstance(mb_url, str) and isinstance(mb_user, str) and isinstance(mb_password, str)
except AssertionError:
    print("You must set the environment variables METABASE_URL, METABASE_USER and METABASE_PASSWORD.")
    exit(1)

mbapi = Metabase_API(mb_url, mb_user, mb_password)
db_schema = 'default$default'
options = {}


def main():
    global options
    options = parseArguments()

    db = {
        'prod': 2,
        'sandbox': 3
    }

    options['database'] = db[options['database']]

    if options['setNames']:
        setNames()
    elif options['clone']:
        clone()
    else:
        print('What do you want?')


def setNames():
    # Set card name and table name in query

    collection = mbapi.get('/api/collection/{}'.format(options['setNames']))
    bsd_type = collection['name']

    if not options['bsdType']:
        options['bsdType'] = bsd_type

    items: list = mbapi.get('/api/collection/{}/items'.format(options['setNames']))['data']

    for item in items:
        if item['model'] == 'card':
            card = mbapi.get('/api/card/{}'.format(item['id']))
            if bsd_type not in card['name']:
                new_name = re.sub(' \([A-Z]+\)$', '', card['name'])
                new_name = new_name + ' ({})'.format(bsd_type)
                card['name'] = new_name
            card = setTableName(card)
            if options['database']:
                card = setDatabaseId(card)

            mbapi.put('/api/card/{}'.format(item['id']), json=card)


def setTableName(card: dict) -> dict:
    table_names = {
        'BSDD': 'Forms',
        'DASRI': 'Bsdasri',
        'BSFF': 'Bsff',
        'VHU': 'Bsvhu',
        'BSDA': 'Bsda'
    }
    new_table_name = table_names[options['bsdType']]
    newTableId = getTableId(new_table_name)
    oldTableName = table_names[mbapi.get('/api/collection/{}'.format(options['clone'][0]))['name']]

    if card['dataset_query']['type'] == 'query':
        card['dataset_query']['query']['source-table'] = newTableId
    elif card['dataset_query']['type'] == 'native':
        to_replace = '"{}"\."{}"'.format(re.escape(db_schema), oldTableName)
        replace_with = '"{}"."{}"'.format(db_schema, new_table_name)
        card['dataset_query']['native']['query'] = re.sub(to_replace, replace_with,
                                                          card['dataset_query']['native']['query'])

    return card


def clone():
    source_collection = options['clone'][0]
    parent_collection = options['clone'][1]

    # Equal ids lead to infinite creation of clones
    assert source_collection != parent_collection

    mbapi.copy_collection(source_collection_id=source_collection,
                          destination_parent_collection_id=parent_collection)

    if options['bsdType'] or options['database']:
        source_collection_name = mbapi.get('/api/collection/{}'.format(source_collection))['name']
        new_collection_id = getCollectionId(parent_collection, source_collection_name)

        if options['bsdType']:
            new_collection = mbapi.get('/api/collection/{}'.format(new_collection_id))
            new_collection['name'] = options['bsdType']
            mbapi.put('/api/collection/{}'.format(new_collection_id), json=new_collection)
            options['setNames'] = new_collection_id
            setNames()

        # setDatabaseId is run if necessary within setNames
        # The lines below only run if only --database is set, not --bsdType
        elif options['database']:
            changeDatabaseInCollection(new_collection_id)


def getCollectionId(parent_id: int, name: str) -> int:
    parent_items = mbapi.get('/api/collection/{}/items'.format(parent_id))['data']
    for item in parent_items:
        if item['name'] == name:
            return item['id']


def getTableId(table_name) -> int:
    tables = mbapi.get('/api/table/')
    for table in tables:
        if table['name'] == table_name and \
                table['db']['id'] == options['database'] and \
                table['schema'] == db_schema:
            return table['id']


def changeDatabaseInCollection(collection_id):
    items: list = mbapi.get('/api/collection/{}/items'.format(collection_id))['data']

    for item in items:
        if item['model'] == 'card':
            card = mbapi.get('/api/card/{}'.format(item['id']))
            card = setDatabaseId(card)
            mbapi.put('/api/card/{}'.format(item['id']), json=card)

def setDatabaseId(card) -> dict:
    card['database_id'] = card['dataset_query']['database'] = options['database']
    return card

def parseArguments() -> dict:
    parser = argparse.ArgumentParser(
        description="Petabase executes mass actions on Metabase cards and dashboards. For now, it's mainly targeted for \
                    Trackdéchets. You can find the item id in the URL of the item. E.g. if URL ends with '/21-item-name', \
                    the id is 21."
    )
    parser.add_argument('--clone', nargs=2, type=int, default=False,
                        help="""Clone a card or a collection (arg 1) as a child to an existing collection (arg 2).
                        Can be used with --database""")
    parser.add_argument('--database', nargs=1, default=False, choices=['prod', 'sandbox'], required=True,
                        help="With --clone : set the database of all the clone cards to 'prod' or 'sandbox' (arg 1).")
    parser.add_argument('--bsdType', nargs=1, required=False, default=False,
                        choices=['BSDD', 'DASRI', 'BSFF', 'VHU', 'BSDA'],
                        help="The target type of BSD: BSDD, DASRI, BSFF, VHU, BSDA")
    parser.add_argument('--setNames', nargs=1, default=False,
                        help="Makes sure the name of the cards in a collection (arg 1) contains the type of BSD. Adds it if necessary.")

    parsed_args = vars(parser.parse_args())

    for arg in parsed_args:
        if isinstance(parsed_args[arg], list) and len(parsed_args[arg]) == 1:
            parsed_args[arg] = parsed_args[arg][0]

    return parsed_args

if __name__ == '__main__':
    main()
