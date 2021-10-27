import argparse
from metabase_api import Metabase_API
import os
import re

mb_url = os.getenv('METABASE_URL')
mb_user = os.getenv('METABASE_USER')
mb_password = os.getenv('METABASE_PASSWORD')
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
    bsdType = collection['name']

    if not options['bsdType']:
        options['bsdType'] = bsdType

    items: list = mbapi.get('/api/collection/{}/items'.format(options['setNames']))['data']

    for item in items:
        if item['model'] == 'card':
            card = mbapi.get('/api/card/{}'.format(item['id']))
            if bsdType not in card['name']:
                newName = re.sub(' \([A-Z]+\)$', '', card['name'])
                newName = newName + ' ({})'.format(bsdType)
                card['name'] = newName
            card = setTableName(card)
            if options['database']:
                card = setDatabaseId(card)

            mbapi.put('/api/card/{}'.format(item['id']), json=card)


def setTableName(card: dict) -> dict:
    tableNames = {
        'BSDD': 'Forms',
        'DASRI': 'Bsdasri',
        'BSFF': 'Bsff',
        'VHU': 'Bsvhu',
        'BSDA': 'Bsda'
    }
    newTableName = tableNames[options['bsdType']]
    newTableId = getTableId(newTableName)
    oldTableName = tableNames[mbapi.get('/api/collection/{}'.format(options['clone'][0]))['name']]

    if card['dataset_query']['type'] == 'query':
        card['dataset_query']['query']['source-table'] = newTableId
    elif card['dataset_query']['type'] == 'native':
        to_replace = '"{}"\."{}"'.format(re.escape(db_schema), oldTableName)
        replace_with = '"{}"."{}"'.format(db_schema, newTableName)
        card['dataset_query']['native']['query'] = re.sub(to_replace, replace_with,
                                                          card['dataset_query']['native']['query'])

    return card


def clone():
    sourceCollection = options['clone'][0]
    parentCollection = options['clone'][1]

    # Equal ids lead to infinite creation of clones
    assert sourceCollection != parentCollection

    mbapi.copy_collection(source_collection_id=sourceCollection,
                          destination_parent_collection_id=parentCollection)

    if options['bsdType'] or options['database']:
        sourceCollectionName = mbapi.get('/api/collection/{}'.format(sourceCollection))['name']
        newCollectionId = getCollectionId(parentCollection, sourceCollectionName)

        if options['bsdType']:
            newCollection = mbapi.get('/api/collection/{}'.format(newCollectionId))
            newCollection['name'] = options['bsdType']
            mbapi.put('/api/collection/{}'.format(newCollectionId), json=newCollection)
            options['setNames'] = newCollectionId
            setNames()

        # setDatabaseId is run if necessary within setNames
        # The lines below only run if only --database is set, not --bsdType
        elif options['database']:
            changeDatabaseInCollection(newCollectionId)


def getCollectionId(parentId: int, name: str) -> int:
    parentItems = mbapi.get('/api/collection/{}/items'.format(parentId))['data']
    for item in parentItems:
        if item['name'] == name:
            return item['id']


def getTableId(tableName) -> int:
    tables = mbapi.get('/api/table/')
    for table in tables:
        if table['name'] == tableName and \
                table['db']['id'] == options['database'] and \
                table['schema'] == db_schema:
            return table['id']


def changeDatabaseInCollection(collectionId):
    items: list = mbapi.get('/api/collection/{}/items'.format(collectionId))['data']

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
