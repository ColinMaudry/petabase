import argparse
from metabase_api import Metabase_API
import sys
import os

mb_url = os.getenv('METABASE_URL')
mb_user = os.getenv('METABASE_USER')
mb_password = os.getenv('METABASE_PASSWORD')
mbapi = Metabase_API(mb_url, mb_user, mb_password)

def main(args):
    options = parseArguments(args)
    if options['setNames']:
        setNames(options)
    elif options['clone']:
        clone(options)
    else:
        print('What do you want?')


def setNames(options):
    import re

    collection = mbapi.get('/api/collection/{}'.format(options['setNames']))
    bsdType = collection['name']

    items: list = mbapi.get('/api/collection/{}/items'.format(options['setNames']))['data']

    for item in items:
        if item['model'] == 'card' and bsdType not in item['name']:
            newName = re.sub(' \([A-Z]+\)$', '', item['name'])
            newName = newName + ' ({})'.format(bsdType)
            item['name'] = newName
            mbapi.put('/api/card/{}'.format(item['id']), json=item)


def clone(options):
    mbapi = Metabase_API(mb_url, mb_user, mb_password)
    mbapi.copy_collection(source_collection_id=options['clone'][0], destination_parent_collection_id=options['clone'][1])

    if options['bsdType']:
        sourceCollectionName = mbapi.get('/api/collection/{}'.format(options['clone'][0]))['name']
        newCollectionId = getCollectionId(options['clone'][1], sourceCollectionName)
        newCollection = mbapi.get('/api/collection/{}'.format(newCollectionId))
        newCollection['name'] = options['bsdType']
        mbapi.put('/api/collection/{}'.format(newCollectionId), json=newCollection)
        setNamesOptions = {
            'setNames': newCollectionId
        }
        setNames(setNamesOptions)



def getCollectionId(parentId: int, name: str) -> int:
    parentItems = mbapi.get('/api/collection/{}/items'.format(parentId))['data']
    for item in parentItems:
        if item['name'] == name:
            return item['id']


def parseArguments(args) -> dict:
    parser = argparse.ArgumentParser(
        description="Petabase executes mass actions on Metabase cards and dashboards. For now, it's mainly targeted for Trackdéchets. You can find the item id in the URL of the item. E.g. if URL ends with '/21-item-name', the id is 21."
    )
    parser.add_argument('--clone', nargs=2, type=int,
                        help="Clone a card or a collection (arg 1) as a child to an existing collection (arg 2).")
    parser.add_argument('--bsdType', nargs='?', required=False, default=None,
                        choices=['BSDD', 'DASRI', 'BSFF', 'VHU', 'BSDA'],
                        help="The target type of BSD: BSDD, DASRI, BSFF, VHU, BSDA")
    parser.add_argument('--setNames', nargs='?',
                        help="Makes sure the name of the cards in a collection (arg 1) contains the type of BSD. Adds it if necessary.")

    parsed_args = vars(parser.parse_args())
    options = {
        'clone': parsed_args.get('clone'),
        'bsdType': parsed_args.get('bsdType'),
        'setNames': parsed_args.get('setNames')
    }

    return options


if __name__ == '__main__':
    main(sys.argv)
