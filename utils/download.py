#!/usr/bin/env python

from datetime import date, timedelta, datetime
from os import getcwd, path
import sys
import argparse
import json

import plaid
from plaid.api import plaid_api
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investment_holdings_get_request_options import InvestmentHoldingsGetRequestOptions
from plaid.model.transactions_refresh_request import TransactionsRefreshRequest


def get_item(client, access_token):
    try:
        request = ItemGetRequest(access_token)
        return client.item_get(request)
    except plaid.ApiException as e:
        response = json.loads(str(e.body))
        print(f"Failed to get item: { response['display_message'] }({ response['error_code'] })")
    return None


def get_investment_holdings(client, access_token, account_id):
    options = InvestmentHoldingsGetRequestOptions()
    if account_id is not None:
        options.account_ids = [account_id]

    try:
        request = InvestmentsHoldingsGetRequest(access_token=access_token,
                                                options=options)
        return client.investments_holdings_get(request)
    except plaid.ApiException as e:
        response = json.loads(str(e.body))
        print(f"Download holdings failed: { response['display_message'] }({ response['error_code'] })")
    return None


def get_investment_transactions(client, access_token, account_id, start_date, end_date):
    try:
        options = InvestmentsTransactionsGetRequestOptions()
        if account_id is not None:
            options.account_ids = [account_id]
            options.count = 500
        request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=options,
        )
        response = client.investments_transactions_get(request)
        resp_dict = response

        # the transactions in the response are paginated, so make multiple calls
        # while increasing the offset to retrieve all transactions
        while len(resp_dict['investment_transactions']) < response['total_investment_transactions']:
            options.offset = len(resp_dict['investment_transactions'])

            request = InvestmentsTransactionsGetRequest(
                access_token=access_token,
                start_date=start_date,
                end_date=end_date,
                options=options
            )
            response = client.investments_transactions_get(request)
            resp_dict['investment_transactions'].extend(response['investment_transactions'])
    except plaid.ApiException as e:
        response = json.loads(str(e.body))
        print(f"Download failed: { response['display_message'] }({ response['error_code'] })")
        resp_dict = None
    return resp_dict


def get_transactions(client, access_token, account_id, start_date, end_date):
    try:
        options = TransactionsGetRequestOptions()
        if account_id is not None:
            options.account_ids = [account_id]
            options.count = 500
        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=options
        )
        response = client.transactions_get(request)
        resp_dict = response

        # The transactions in the response are paginated, so make multiple
        # calls while increasing the offset to retrieve all transactions
        while len(resp_dict['transactions']) < response['total_transactions']:
            options.offset = len(resp_dict['transactions'])

            request = TransactionsGetRequest(
                access_token=access_token,
                start_date=start_date,
                end_date=end_date,
                options=options
            )
            response = client.transactions_get(request)
            resp_dict['transactions'].extend(response['transactions'])
    except plaid.ApiException as e:
        response = json.loads(str(e.body))
        print(f"Download failed: { response['display_message'] }({ response['error_code'] })")
        resp_dict = None
    return resp_dict


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
    parser.add_argument('-l', '--list', action='store_true', help="List accounts")
    parser.add_argument("-r", "--refresh", action='store_true',
                        help="Force plaid to refresh data immediately before downloading")
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
    args.directory = getcwd() if args.directory is None else args.directory

    if args.list is True:
        print("\n".join(args.config['accounts'].keys()))
        sys.exit(0)

    # If accounts are not specified, add all of them
    if args.accounts:
        for acct in args.accounts:
            if acct not in args.config['accounts'].keys():
                parser.error(f"Unknown Account: { acct }")
    else:
        # All accounts
        args.accounts = args.config['accounts'].keys()

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
        access_token = args.config['accounts'][acct]['access_token']
        account_id = None
        output = {}

        if args.refresh:
            try:
                request = TransactionsRefreshRequest(access_token=access_token)
                response = client.transactions_refresh(request)
            except plaid.ApiException as e:
                response = json.loads(str(e.body))
                print(f"refresh failed: { response['display_message'] }({ response['error_code'] })")

        # Determine last plaid update and supported api calls
        print(f"Downloading { acct }...")
        response = get_item(client, access_token)
        if response is not None:
            output['resp_item'] = response.to_dict()
            products = output['resp_item']['item']['products']

            if 'account_id' in args.config['accounts'][acct]:
                account_id = args.config['accounts'][acct]['account_id']

            # Download Transactions
            if "transactions" in products:
                print("..transactions")
                transactions = get_transactions(client, access_token, account_id, args.start_date, args.end_date)
                if transactions is not None:
                    output['resp_transactions'] = transactions.to_dict()

            # Download Investment Transactions
            if "investments" in products:
                print("..investment transactions")
                investment_transactions = get_investment_transactions(client, access_token, account_id, args.start_date, args.end_date)
                if investment_transactions is not None:
                    output['resp_investment_transactions'] = investment_transactions.to_dict()

                print("..investment holdings")
                holdings = get_investment_holdings(client, access_token, account_id)
                if holdings is not None:
                    output['resp_investment_holdings'] = holdings.to_dict()

            # Save to file
            with open(path.join(args.directory, f"{date.today()}_{acct}_plaid_download.json"), "w") as out_fd:
                json.dump(output, out_fd, cls=DateEncoder)


if __name__ == '__main__':
    main()
