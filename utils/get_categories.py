#!/usr/bin/env python

import argparse
import json

import plaid
from plaid.api import plaid_api


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config_file', default='settings.json',
                        help='Config file location, default (settings.json)')
    args = parser.parse_args()

    # Load settings config file
    with open(args.config_file, "r") as config_fd:
        args.config = json.load(config_fd)

    plaid_config = plaid.Configuration(
        host=args.config['plaid_config']['host'],
        api_key=args.config['plaid_config']['api_key'],
    )
    api_client = plaid.ApiClient(plaid_config)
    client = plaid_api.PlaidApi(api_client)

    categories = client.categories_get({})
    for category in categories['categories']:
        # print(category)
        c = ':'.join(category['hierarchy']).replace(
            "'", "").replace(',', '').replace(' ', '-')
        print(f'1900-01-01 open Expenses:{c} USD')


if __name__ == '__main__':
    main()
