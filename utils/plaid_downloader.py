#!/usr/bin/env python

from datetime import date, timedelta, datetime
from os import getcwd, path
import sys
import argparse
import json

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions


def get_transactions(client, access_token, account_id, start_date, end_date):
    options = TransactionsGetRequestOptions()
    if account_id is not None:
        options.account_ids = [account_id]
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        options=options,
    )
    response = client.transactions_get(request)
    rc = response

    # the transactions in the response are paginated, so make multiple calls
    # while increasing the offset to retrieve all transactions
    while len(rc['transactions']) < response['total_transactions']:
        options.offset = len(rc['transactions'])

        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=options
        )
        response = client.transactions_get(request)
        rc['transactions'].extend(response['transactions'])

    return rc


class DateEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, date):
            return (str(z))
        else:
            return super().default(z)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--accounts', dest='accounts', type=str,
                        action='append', help='List of account names to download, defaults to all')
    parser.add_argument('-c', '--config', dest='config_file', default='settings.json',
                        help='Config file location, default (settings.json)')
    parser.add_argument('-d', '--directory',
                        help="Directory to store downloaded plaid files")
    parser.add_argument('-l', '--list', action='store_true',
                        help="List accounts")
    parser.add_argument('-s', '--start-date', dest='start_date', default=str(date.today() -
                        timedelta(1)), help='First day of history to attempt to download, default to yesterday')
    parser.add_argument('-e', '--end-date', dest='end_date', default=str(date.today()),
                        help='Last day of history to attempt to download, defaults to today')
    args = parser.parse_args()

    # Load settings config file
    with open(args.config_file, "r") as config_fd:
        args.config = json.load(config_fd)

    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()

    # If accounts are not specified, add all of them
    if args.accounts:
        for acct in args.accounts:
            if acct not in args.config['accounts'].keys():
                parser.error(f"Unknown Account: { acct }")
    else:
        # All accounts
        args.accounts = args.config['accounts'].keys()

    if args.directory is None:
        args.directory = getcwd()

    if args.list is True:
        for acct in args.accounts:
            print(f"{acct}")
        sys.exit(0)

    return args


def main():
    args = parse_args()

    plaid_config = plaid.Configuration(
        host=args.config['plaid_config']['host'],
        api_key=args.config['plaid_config']['api_key'],
    )
    api_client = plaid.ApiClient(plaid_config)
    client = plaid_api.PlaidApi(api_client)

    for acct in args.accounts:
        account_id = None
        if 'account_id' in args.config['accounts'][acct]:
            account_id = args.config['accounts'][acct]['account_id']
        transactions = get_transactions(client, args.config['accounts'][acct]['access_token'],
                                        account_id,
                                        args.start_date,
                                        args.end_date)
        with open(path.join(args.directory, f"{date.today()}_{acct}_plaid_download.json"), "w") as out_fd:
            json.dump(transactions.to_dict(), out_fd, cls=DateEncoder)


if __name__ == '__main__':
    main()
