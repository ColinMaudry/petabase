import argparse
from metabase_api import Metabase_API
from metabasepy import Client
import sys
import os

mb_url = os.getenv('METABASE_URL')
mb_user = os.getenv('METABASE_USER')
mb_password = os.getenv('METABASE_PASSWORD')


def main(args):
    options = parseArguments(args)
    if options['setNames'] and options['setNames']:
        setNames(options)


def setNames(options):
    import re

    mbapi = Metabase_API(mb_url, mb_user, mb_password)
    collection = mbapi.get('/api/collection/{}'.format(options['setNames']))
    bsdType = collection['name']

    items: list = mbapi.get('/api/collection/{}/items'.format(options['setNames']))['data']

    for item in items:
        if item['model'] == 'card' and bsdType not in item['name']:
            newName = re.sub(' \([A-Z]+\)$', '', item['name'])
            newName = newName + ' ({})'.format(bsdType)
            item['name'] = newName
            mbapi.put('/api/card/{}'.format(item['id']), json=item)


def parseArguments(args) -> dict:
    parser = argparse.ArgumentParser(
        description="Petabase executes mass actions on Metabase cards and dashboards. For now, it's mainly targeted for Trackdéchets."
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


def listCollection(name: str, instance: str):
    mbpy = Client(username=mb_user, password=mb_password, base_url=mb_url)
    mbpy.authenticate()


def copyCollection(source: int, target: int):
    mbapi = Metabase_API(mb_url, mb_user, mb_password)
    mbapi.copy_collection(source_collection_id=source, destination_parent_collection_id=target)


if __name__ == '__main__':
    main(sys.argv)
