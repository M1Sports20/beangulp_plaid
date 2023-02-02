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
    parser.add_argument('-n', '--fetch-days', type=int, dest='fetch_days', default=30,
                        help='Number of days of transaction history to fetch')
    args = parser.parse_args()

    # Load settings config file
    with open(args.config_file, "r") as config_fd:
        args.config = json.load(config_fd)

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

class PlaidDownloader:
    def __init__(self, name, access_token, account_id, plaid_client, download_delta=timedelta(days=30), refresh=False):
        self.account_name = name
        self.access_token = access_token
        self.account_id = account_id
        self.plaid_client = plaid_client
        self.download_delta = download_delta
        self.refresh = refresh

    def download(self, filename):
        """
        Download the most recent statement for an account.

        Args:
          filename: Where to place the downloaded statement
        Returns:
          A boolean: True if successful, False if failed
          """
        output = {}

        end_date = date.today()
        start_date = end_date - self.download_delta

        if self.refresh:
            try:
                request = TransactionsRefreshRequest(access_token=self.access_token)
                response = self.plaid_client.transactions_refresh(request)
            except plaid.ApiException as e:
                response = json.loads(str(e.body))
                print(f"refresh failed: { response['display_message'] }({ response['error_code'] })")

        # Determine supported api calls
        print(f"Downloading { self.account_name }...")
        response = get_item(self.plaid_client, self.access_token)
        if response is not None:
            output['resp_item'] = response.to_dict()
            products = output['resp_item']['item']['products']

            # Download Transactions
            if "transactions" in products:
                print("..transactions")
                transactions = get_transactions(self.plaid_client, self.access_token, self.account_id, start_date, end_date)
                if transactions is not None:
                    output['resp_transactions'] = transactions.to_dict()

            # Download Investment Transactions
            if "investments" in products:
                print("..investment transactions")
                investment_transactions = get_investment_transactions(self.plaid_client, self.access_token, self.account_id, start_date, end_date)
                if investment_transactions is not None:
                    output['resp_investment_transactions'] = investment_transactions.to_dict()

                print("..investment holdings")
                holdings = get_investment_holdings(self.plaid_client, self.access_token, self.account_id)
                if holdings is not None:
                    output['resp_investment_holdings'] = holdings.to_dict()

            # Save to file
            with open(filename, "w") as out_fd:
                json.dump(output, out_fd, cls=DateEncoder)

        return True

    def filename_suffix(self):
        return "plaid.json"

    def name(self):
        """
        Return a name for the account, suitable for use as a portion of a
        filename
        """
        return self.account_name

def main():
    args = parse_args()

    plaid_config = plaid.Configuration(
        host=args.config['plaid_config']['host'],
        api_key=args.config['plaid_config']['api_key'],
    )
    api_client = plaid.ApiClient(plaid_config)
    client = plaid_api.PlaidApi(api_client)

    for account_name in args.accounts:
        account_config = args.config['accounts'][account_name]
        downloader = PlaidDownloader(
            name=account_name,
            access_token=account_config['access_token'],
            account_id=account_config['account_id'] if 'account_id' in account_config else None,
            plaid_client=client,
            download_delta=timedelta(args.fetch_days),
            refresh=args.refresh
        )
        downloader.download(path.join(args.directory, f"{date.today()}_{account_name}_plaid_download.json"))

if __name__ == '__main__':
    main()
