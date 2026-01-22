#!/usr/bin/env python

import argparse
import os
import sys
import json
import logging
from datetime import datetime, date
from tabulate import tabulate
from pathlib import Path

from plaid_api import PlaidApi, Products

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
logger.addHandler(handler)


def print_save_output(output: Path, data: dict):
    if output:
        with output.open("w", encoding="utf-8") as f:
            json.dump(data, f, default=str, indent=2)
    else:
        print(json.dumps(data, default=str, indent=2))


def cmd_refresh_transactions(plaid_api, args: argparse.Namespace):
    plaid_api.transactions_refresh(args.access_token)


def cmd_refresh_investments(plaid_api, args: argparse.Namespace):
    plaid_api.investment_refresh(args.access_token)


def cmd_download_transactions(plaid_api, args: argparse.Namespace):
    data = {}
    data['transactions'] = plaid_api.get_transactions(args.access_token, args.account_ids, args.start_date, args.end_date)
    print_save_output(args.output,data)


def cmd_download_investments_transactions(plaid_api, args: argparse.Namespace):
    data = {}
    data['investment_transactions'] = plaid_api.get_investment_transactions(args.access_token, args.account_ids, args.start_date, args.end_date)
    print_save_output(args.output, data)


def cmd_download_investments_holdings(plaid_api, args: argparse.Namespace):
    data = {}
    data['investment_holdings'] = plaid_api.get_investment_holdings(args.access_token, args.account_ids)
    print_save_output(args.output, data)


def cmd_download(plaid_api, args: argparse.Namespace):
    data = {}
    data['transactions'] = plaid_api.get_transactions(args.access_token, args.account_ids, args.start_date, args.end_date)
    data['investment_transactions'] = plaid_api.get_investment_transactions(args.access_token, args.account_ids, args.start_date, args.end_date)
    data['investment_holdings'] = plaid_api.get_investment_holdings(args.access_token, args.account_ids)
    print_save_output(args.output, data)


def cmd_remove_accounts(plaid_api, args: argparse.Namespace):
    plaid_api.remove_item(args.access_token)


def cmd_list_accounts(plaid_api, args: argparse.Namespace):
    accounts = plaid_api.get_accounts(args.access_token).accounts
    table = []
    for a in accounts:
        balance = f"${a.balances.current:,.2f}"
        table.append([f"{a.name}({a.mask})", a.type, a.subtype, balance, a.account_id])
    header = ["Name", "Type", "Subtype", "Balance", "Account Id"]
    print(tabulate(table, headers=header, tablefmt="github"))


def cmd_create_link(plaid_api, args: argparse.Namespace):
    # Create Link

    product_enums = None
    if args.products:
        product_enums = []
        product_enums = [Products(p) for p in args.products]

    if args.command == 'create-link':
        link_token = plaid_api.create_link_token(client_user_id=args.user_id, products=product_enums)
    else:
        link_token = plaid_api.update_link_token(client_user_id=args.user_id, access_token=args.access_token, products=product_enums)

    print(f"Complete registration @ {link_token.hosted_link_url}")
    print("Press any key to continue, or 'x' to exit.")
    ch = sys.stdin.read(1)
    if ch.lower() == 'x':
        print("Exiting.")
        sys.exit(0)

    # We have the link token, if the user finished the registration we can
    # call get_link token to get updated information provided by users
    # registration
    link_token = plaid_api.get_link_token_from_link_token(link_token.link_token)
    public_token = link_token.link_sessions[0].results['item_add_results'][0]['public_token']

    # Exchange Public token for access_token
    access_token = plaid_api.exchange_public_token_to_access_token(public_token)
    print(tabulate([[access_token.access_token]], headers=["Access Token"], tablefmt="github"))


def build_parser() -> argparse.ArgumentParser:
    def valid_date(s: str) -> date:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            raise argparse.ArgumentTypeError(f"Not a valid date: '{s}'. Expected YYYY-MM-DD.")

    parser = argparse.ArgumentParser(description="Plaid Link Commands", formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=40))
    parser.add_argument("-c", "--client-id", default=os.environ.get("CLIENT_ID"), help="Plaid client_id, (or use env var CLIENT_ID)")
    parser.add_argument("-s", "--secret", default=os.environ.get("SECRET"), help="Plaid secret, (or use env var SECRET)")
    parser.add_argument("--production", action="store_true", help="Enable production mode")
    parser.add_argument("--prompt-charges", action="store_true", help="Prompt/Confirm whenever a charge will incur")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (use -v, -vv)")

    sub = parser.add_subparsers(title="commands", dest="command")

    # create_link subcommand
    p_create = sub.add_parser("create-link", help="Obtain an access-token by creating a new Link via Plaid")
    p_create.add_argument("-u", "--user-id", required=True, help="Client user id for link token")
    p_create.add_argument("-p", "--products", nargs="+", choices=[p.value for p in Products], help="Products to associate with this link")
    p_create.set_defaults(func=cmd_create_link)

    # update_link subcommand
    p_update = sub.add_parser("update-link", help="Update an access-token by updating a Link via Plaid")
    p_update.add_argument("-u", "--user-id", required=True, help="Client user id for link token")
    p_update.add_argument("--access-token", required=True, help="Update existing token via Plaid Link")
    p_update.add_argument("-p", "--products", nargs="*", choices=list(Products), type=lambda s: Products(s.lower()), help="Products to add to link")
    p_update.set_defaults(func=cmd_create_link)

    # remove_items subcommand
    p_remove = sub.add_parser("remove-link", help="Remove accounts associated with access-code")
    p_remove.add_argument("--access-token", required=True, help="access_token")
    p_remove.set_defaults(func=cmd_remove_accounts)

    # list_accounts subcommand
    p_list = sub.add_parser("list-accounts", help="List accounts")
    p_list.add_argument("--access-token", required=True, help="access_token from which to list accounts")
    p_list.set_defaults(func=cmd_list_accounts)

    # refresh-transactions subcommand
    p_transactions_refresh = sub.add_parser("refresh-transactions", help="Force sync server to refresh transactions with bank")
    p_transactions_refresh.add_argument("--access-token", required=True, help="access_token")
    p_transactions_refresh.set_defaults(func=cmd_refresh_transactions)

    # download-transactions subcommand
    p_download_transactions = sub.add_parser("download-transactions", help="Download transaction data")
    p_download_transactions.add_argument("--access-token", required=True, help="access_token")
    p_download_transactions.add_argument("--account-ids", nargs="*", help="account_ids to download, if not specified all accounts with token are downloaded.")
    p_download_transactions.add_argument("--start-date", type=valid_date, help="start date to get transactions (format in YYYY-MM-DD)")
    p_download_transactions.add_argument("--end-date", type=valid_date, default=date.today(), help="end date to get transactions format in (YYYY-MM-DD)")
    p_download_transactions.add_argument("--output", type=Path, help="Save json to file, instead of stdout")
    p_download_transactions.set_defaults(func=cmd_download_transactions)

    # refresh investments subcommand
    p_investment_refresh = sub.add_parser("refresh-investments", help="Force sync server to refresh transactions with investment bank")
    p_investment_refresh.add_argument("--access-token", required=True, help="access_token")
    p_investment_refresh.set_defaults(func=cmd_refresh_investments)

    # download-investment-transactions subcommand
    p_download_investments_transactions = sub.add_parser("download-investment-transactions", help="Download investment transaction data")
    p_download_investments_transactions.add_argument("--access-token", required=True, help="access_token")
    p_download_investments_transactions.add_argument("--account-ids", nargs="*", help="account_ids to download, if not specified all accounts with token are downloaded.")
    p_download_investments_transactions.add_argument("--start-date", type=valid_date, help="start date to get transactions (format in YYYY-MM-DD)")
    p_download_investments_transactions.add_argument("--end-date", type=valid_date, default=date.today(), help="end date to get transactions format in (YYYY-MM-DD)")
    p_download_investments_transactions.add_argument("--output", type=Path, help="Save json to file, instead of stdout")
    p_download_investments_transactions.set_defaults(func=cmd_download_investments_transactions)

    # download-investment-holdings subcommand
    p_download_investments_holdings = sub.add_parser("download-investment-holdings", help="Download investment holdings data")
    p_download_investments_holdings.add_argument("--access-token", required=True, help="access_token")
    p_download_investments_holdings.add_argument("--account-ids", nargs="*", help="account_ids to download, if not specified all accounts with token are downloaded.")
    p_download_investments_holdings.add_argument("--output", type=Path, help="Save json to file, instead of stdout")
    p_download_investments_holdings.set_defaults(func=cmd_download_investments_holdings)

    # download-all subcommand
    p_download = sub.add_parser("download", help="Download all available information into nested json object")
    p_download.add_argument("--access-token", required=True, help="access_token")
    p_download.add_argument("--account-ids", nargs="*", help="account_ids to download, if not specified all accounts with token are downloaded.")
    p_download.add_argument("--start-date", type=valid_date, help="start date to get transactions (format in YYYY-MM-DD)")
    p_download.add_argument("--end-date", type=valid_date, default=date.today(), help="end date to get transactions format in (YYYY-MM-DD)")
    p_download.add_argument("--output", type=Path, help="Save json to file, instead of stdout")
    p_download.set_defaults(func=cmd_download)

    return parser


class PlaidDownloader:
    def __init__(self, name, client_id, secret, access_token, account_id, start_date=None, end_date=date.today(), is_investment=True, is_bank=True):
        self.client_id = client_id
        self.secret = secret
        self.access_token = access_token
        self.account_id = account_id
        self.account_name = name
        self.is_investment = is_investment
        self.is_bank = is_bank
        self.end_date = end_date
        self.start_date = start_date

    def download(self, filename):
        plaid_api = PlaidApi(client_id=self.client_id, secret=self.secret, production=True)
        data = {}
        if self.is_bank:
            data['transactions'] = plaid_api.get_transactions(self.access_token, self.account_id, self.start_date, self.end_date)
        if self.is_investment:
            data['investment_transactions'] = plaid_api.get_investment_transactions(self.access_token, self.account_id, self.start_date, self.end_date)
            data['investment_holdings'] = plaid_api.get_investment_holdings(self.access_token, self.account_id)
        print_save_output(filename, data)

    def filename_suffix(self):
        return "plaid.json"

    def name(self):
        return self.account_name


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        sys.exit(0)

    if args.verbose >= 2:
        logging.getLogger("plaid_api").setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    elif args.verbose == 1:
        logging.getLogger("plaid_api").setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
    else:
        logging.getLogger("plaid_api").setLevel(logging.WARNING)
        logger.setLevel(logging.WARNING)

    if args.client_id is None:
        raise ValueError("CLIENT_ID env var must be set, or use '--client-id'")
    if args.secret is None:
        raise ValueError("SECRET env var must be set, or use '--secret'")

    api = PlaidApi(client_id=args.client_id, secret=args.secret, production=args.production, prompt_charges=args.prompt_charges)
    exit_code = args.func(api, args)
    sys.exit(exit_code)
