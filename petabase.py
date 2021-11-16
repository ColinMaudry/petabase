import argparse
from metabase_api import Metabase_API
import os
import re
import logging
from datetime import datetime

time = datetime.now().strftime("%Y-%m-%dT%H:%M")

# With python 3.9+, add encoding='utf-8'
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

mb_url = os.getenv('METABASE_URL')
mb_user = os.getenv('METABASE_USER')
mb_password = os.getenv('METABASE_PASSWORD')

try:
    assert isinstance(mb_url, str) and isinstance(mb_user, str) and isinstance(mb_password, str)
except AssertionError:
    logging.error('ERROR: You must set the environment variables METABASE_URL, METABASE_USER and METABASE_PASSWORD.')
    exit(1)

mbapi = Metabase_API(mb_url, mb_user, mb_password)
logging.info("Metabase API connection established")
db_schema = 'default$default'
options = {}
table_names = {
    'BSDD': 'Forms',
    'DASRI': 'Bsdasri',
    'BSFF': 'Bsff',
    'VHU': 'Bsvhu',
    'BSDA': 'Bsda',
}


def main():
    global options
    options = parseArguments()

    db = {
        'prod': 2,
        'sandbox': 3,
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

    logging.info("Retrieving collection data (%s)...", options['setNames'])
    collection = mbapi.get('/api/collection/{}'.format(options['setNames']))
    bsd_type = collection['name']
    logging.info("-> Retrieved collection '%s' (%s) for setNames.", bsd_type, options['setNames'])

    if not options['bsdType']:
        options['bsdType'] = bsd_type

    logging.debug("Retrieving collection items (%s)...", options['setNames'])
    items: list = mbapi.get('/api/collection/{}/items'.format(options['setNames']))['data']

    for item in items:
        if item['model'] == 'card':
            logging.debug("-> Retrieving card data '%s'...", item['name'])
            card = mbapi.get('/api/card/{}'.format(item['id']))
            if bsd_type not in card['name']:
                new_name = re.sub(' \([A-Z]+\)$', '', card['name'])
                new_name = new_name + ' ({})'.format(bsd_type)
                card['name'] = new_name
            card = setTableName(card)
            if options['database']:
                card = setDatabaseId(card)
                # The field id will be replaced, BUT some may not exist for new bsd type (= in the new table)
                card = replaceFieldIdsInCard(card)
            logging.info("-- Pushing card data '%s' with new properties...", card['name'])
            mbapi.put('/api/card/{}'.format(item['id']), json=card)


def setTableName(card: dict) -> dict:
    new_table_name = table_names[options['bsdType']]
    newTableId = getTableId(new_table_name)
    oldTableName = table_names[mbapi.get('/api/collection/{}'.format(options['clone'][0]))['name']]

    if card['dataset_query']['type'] == 'query':
        card['dataset_query']['query']['source-table'] = newTableId

    # TODO: replace table name in native queries doesn't seem to work
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

    # TODO: check that the target parent collection doesn't already have a collecton with the
    #  new collection name

    logging.info('Copying collection %s (and all children) to '
                 'parent collection %s...', source_collection, parent_collection)
    mbapi.copy_collection(source_collection_id=source_collection,
                          destination_parent_collection_id=parent_collection)
    source_collection_name = mbapi.get('/api/collection/{}'.format(source_collection))['name']

    # If there is a database change, query fields and tables will need to be updated
    if options['database']:
        global fields
        fields = mbapi.get('/api/database/{}/fields'.format(options['database']))
        global tables
        tables = [table for table in mbapi.get('/api/table/') if table['db_id'] == options['database']]

    if options['bsdType'] or options['database']:
        new_collection_id = getCollectionId(parent_collection, source_collection_name)
        logging.info("-> New collection %s created.", new_collection_id)

        if options['bsdType']:
            new_collection = mbapi.get('/api/collection/{}'.format(new_collection_id))
            new_collection['name'] = options['bsdType']
            logging.info("Pushing clone collection %s with new name '%s'...", new_collection['id'],
                         new_collection['name'])
            mbapi.put('/api/collection/{}'.format(new_collection_id), json=new_collection)
            options['setNames'] = new_collection_id
            setNames()

        # setDatabaseId is run if necessary within setNames
        # The lines below only run if only --database is set, not --bsdType
        elif options['database']:
            options['bsdType'] = source_collection_name
            changeDatabaseInCollection(new_collection_id)


def getCollectionId(parent_id: int, name: str) -> int:
    parent_items = mbapi.get('/api/collection/{}/items'.format(parent_id))['data']
    for item in parent_items:
        if item['name'] == name:
            item_id = item['id']
            assert isinstance(item_id, int)
            return item_id


def getTableId(table_name) -> int:
    for table in tables:
        if table['name'] == table_name and \
                table['db']['id'] == options['database'] and \
                table['schema'] == db_schema:
            table_id = table['id']
            assert isinstance(table_id, int)
            return table_id


def changeDatabaseInCollection(collection_id):
    logging.info('Retrieving item list of collection %s for database name change...', collection_id)
    items: list = mbapi.get('/api/collection/{}/items'.format(collection_id))['data']
    logging.info('-> Items list retrieved (%s items)', len(items))
    new_table_id = getTableId(table_names[options['bsdType']])
    assert isinstance(new_table_id, int)

    for item in items:
        if item['model'] == 'card':
            card = mbapi.get('/api/card/{}'.format(item['id']))
            logging.info('-> Updating data of card {}...'.format(card['id']))
            card = setDatabaseId(card)
            # Only for queries, not handwritten SQL
            if card['dataset_query']['type'] == 'query':
                card = setTableId(card, new_table_id)
                card = replaceFieldIdsInCard(card)
            else:
                # This info only appears if bsdType is NOT set (= if clone without change of bsdType)
                logging.info(
                    '-? Card https://analytics.trackdechets.beta.gouv.fr/question/%s is a native '
                    'SQL query, no update of table and fields.',
                    card['id'])
            logging.info("-- Pushing card '%s' (%s) with new database id (%s)...", card['name'], card['id'],
                         card['database_id'])
            mbapi.put('/api/card/{}'.format(card['id']), json=card)


def setDatabaseId(card) -> dict:
    card['database_id'] = card['dataset_query']['database'] = options['database']
    return card


def setTableId(card, table_id: int) -> dict:
    card['dataset_query']['query']['source-table'] = card['table_id'] = table_id
    return card


def replaceFieldIdsInCard(card) -> dict:
    if card['dataset_query']['type'] == 'query':
        assert isinstance(options['database'], int)

        query = card['dataset_query']['query']
        params = ['aggregation', 'breakout', 'filter']

        for param in query.keys():
            if param in params:
                query[param] = replaceFieldIdsInList(query[param])
            elif param == 'joins':
                logging.warning('WARNING: https://analytics.trackdechets.beta.gouv.fr/question/{} has '
                                'a join that requires a manual update.', card['id'])
        card['dataset_query']['query'] = query
        return card
    else:
        logging.warning(
            'WARNING: If it\'s a bsdType change, card https://analytics.trackdechets.beta.gouv.fr/question/%s query fields '
            'need to be updated manually',
            card['id'])
        return card


def replaceFieldId(field_id) -> int:

    # the fields array objects use display names for field and table even
    # if they are called them "name", so we need to match on display name
    table_name = getTargetTableName()
    field_name = mbapi.get('/api/field/{}'.format(field_id))['display_name']

    for field in fields:
        if field['table_name'] == table_name and field['name'] == field_name:
            new_field_id = field['id']
            try:
                assert isinstance(new_field_id, int)
            except AssertionError:
                logging.warning("Warning: field {} doesn't exist in table {} and "
                                "database {}".format(field_name, table_name, options['database']))
            return new_field_id


def replaceFieldIdsInList(query: list):
    new_list = []
    for value in query:
        if not isinstance(value, list) or not isinstance(value, dict):
            new_list.append(value)
        elif isinstance(value, list):
            if value[0] == 'field':
                value[1] = replaceFieldId(value[1])
            else:
                value = replaceFieldIdsInList(value)
            new_list.append(value)

    return new_list


def getTargetTableName() -> str:
    target_table_name = ""

    for table in tables:
        if table['name'] == table_names[options['bsdType']] and table['db_id'] == options['database']:
            target_table_name = table['display_name']
            break
    assert len(target_table_name) > 0
    return target_table_name


def parseArguments() -> dict:
    parser = argparse.ArgumentParser(
        description="Petabase executes mass actions on Metabase cards and dashboards. "
                    "For now, it's mainly targeted for Trackdéchets. You can find the item id in the URL of the item. "
                    "E.g. if URL ends with '/21-item-name', the id is 21."
    )
    parser.add_argument('--clone', nargs=2, type=int, default=False,
                        help="""Clone a card or a collection (arg 1) as a child to an existing collection (arg 2).
                        Can be used with --database""")
    parser.add_argument('--database', nargs=1, default=False, choices=['prod', 'sandbox'], required=True,
                        help="Mandatory with --clone: set the database of all the clone cards to "
                             "'prod' or 'sandbox' (arg 1).")
    parser.add_argument('--bsdType', nargs=1, required=False, default=False,
                        choices=['BSDD', 'DASRI', 'BSFF', 'VHU', 'BSDA'],
                        help="The target type of BSD: BSDD, DASRI, BSFF, VHU, BSDA")
    parser.add_argument('--setNames', nargs=1, default=False,
                        help="Makes sure the name of the cards in a collection (arg 1) contains the type of BSD. "
                             "Adds it if necessary.")

    parsed_args = vars(parser.parse_args())

    # If an arg is a list of one value, make it a string/number
    for arg in parsed_args:
        if isinstance(parsed_args[arg], list) and len(parsed_args[arg]) == 1:
            parsed_args[arg] = parsed_args[arg][0]

    return parsed_args


if __name__ == '__main__':
    main()
