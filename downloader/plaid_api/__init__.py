#!/usr/bin/env python

from enum import Enum
import json
import logging
import sys
from datetime import date, timedelta
from typing import Iterable

import plaid
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.item_remove_request import ItemRemoveRequest
from plaid.model.link_token_get_request import LinkTokenGetRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.country_code import CountryCode
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions
from plaid.model.transactions_refresh_request import TransactionsRefreshRequest
from plaid.model.products import Products as PlaidProducts

from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investment_holdings_get_request_options import InvestmentHoldingsGetRequestOptions
from plaid.model.investments_refresh_request import InvestmentsRefreshRequest

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(handler)


class Products(Enum):
    BALANCE = 'balance'
    INVESTMENTS = 'investments'
    LIABILITIES = 'liabilities'
    TRANSACTIONS = 'transactions'


class PlaidApi:
    class ChargeType(Enum):
        SUBSCRIBE = 1
        ONE_TIME = 2
        REMOVE = 3

    PricingFees = {
        "transactions": 0.30,
        "investments": 0.35 + 0.18,
        "liabilities": 0.20,
        "refresh": 0.12
    }

    def __init__(self, client_id: str, secret: str, production: bool = False, prompt_charges: bool = True):
        self.client = self._get_client(client_id, secret, production)
        self.prompt_charges = production and prompt_charges

    def _get_client(self, client_id: str, secret: str, production: bool = False):
        """ Returns a client API object used to Send/Recv Requests """

        configuration = plaid.Configuration(
            host=plaid.Environment.Production if production else plaid.Environment.Sandbox,
            api_key={
                'clientId': client_id,
                'secret': secret,
            }
        )
        api_client = plaid.ApiClient(configuration)
        return plaid_api.PlaidApi(api_client)

    def _log_api(self, data):
        """ Debug function to print out requests/responses of all Plaid API calls """

        name = type(data).__name__
        pretty_json = json.dumps(data.to_dict(), indent=2, default=str)
        logger.debug(f"{name}\n" + pretty_json)

    def _print_charge_warning(self, charge_type: ChargeType, fee: float = 99999):
        """ Display warning if we are going to produce a charge """

        if self.prompt_charges:
            if charge_type == self.ChargeType.SUBSCRIBE:
                print(f"You are about to incur a subscription charge of ~${fee:,.2f} per month.")
            elif charge_type == self.ChargeType.ONE_TIME:
                print(f"You are about to incur a one-time charge of ~${fee:,.2f}.")
            elif charge_type == self.ChargeType.REMOVE:
                print("You are about to remove a subscription charge.")
            else:
                raise ValueError(f"Unknown ChargeType, '{charge_type}'")

            while True:
                ans = input("Would you like to continue (y/n)?").strip().lower()
                if ans in ('y', 'yes'):
                    return True
                if ans in ('n', 'no'):
                    sys.exit(0)
                print("Please enter 'y' or 'n'.")

    def create_link_token(self, client_user_id: str, products: 'Products' | Iterable['Products'] | None = None, client_name: str = "BeanCount"):
        """ Create a short lived link token.  This token is passed to the Link,
            plaids authentication link with banks """

        if products is None:
            products = Products.TRANSACTIONS

        if isinstance(products, Products):
            products = [products]

        request = LinkTokenCreateRequest(
            products=[PlaidProducts(p.value) for p in products],
            hosted_link={},  # use plaids hosted link
            client_name=client_name,
            country_codes=[CountryCode('US')],
            language='en',
            user=LinkTokenCreateRequestUser(client_user_id=client_user_id)
        )
        self._log_api(request)
        response = self.client.link_token_create(request)
        self._log_api(response)
        logger.info(f"public link_token request - token: '{response.link_token}'")
        return response

    def update_link_token(self, client_user_id: str, access_token: str, products=None, client_name: str = "BeanCount"):
        """ Create a short lived link token.  This token is passed to the Link,
            plaids authentication link with banks """

        # TODO: There is something wrong here when updating the product it doesn't seem to add
        raise NotImplementedError("There is something wrong and update doesn't seem to add new investment")

        if isinstance(products, Products):
            products = [products]

        request = LinkTokenCreateRequest(
            hosted_link={},  # Use plaids Hosted Link
            # additional_consented_products=[PlaidProducts(p.value) for p in products],
            access_token=access_token,
            client_name=client_name,
            country_codes=[CountryCode('US')],
            language='en',
            user=LinkTokenCreateRequestUser(client_user_id=client_user_id)
        )
        self._log_api(request)
        response = self.client.link_token_create(request)
        self._log_api(response)
        logger.info(f"update link_token requst - token: '{response.link_token}'")
        return response

    def get_link_token_from_link_token(self, link_token: str):
        """ Update link token with more data, this is used after Link
            (plaids auth website) authenticates with a bank. """

        request = LinkTokenGetRequest(link_token)
        self._log_api(request)
        response = self.client.link_token_get(request)
        self._log_api(response)
        return response

    def exchange_public_token_to_access_token(self, public_token: str):
        """ Exchanges a temporary link_token for a perminate access token.
            This will only succeed if the link token has been populated by
            Plaids Links authentication portal """

        self._print_charge_warning(self.ChargeType.SUBSCRIBE)
        request = ItemPublicTokenExchangeRequest(public_token)
        self._log_api(request)
        response = self.client.item_public_token_exchange(request)
        self._log_api(response)
        logger.info(f"exchange public link token for access_token - token: '{response.access_token}'")
        return response

    def get_accounts(self, access_token: str):
        """ List Accounts and information associated with the account. """

        request = AccountsGetRequest(access_token)
        self._log_api(request)
        response = self.client.accounts_get(request)
        self._log_api(response)
        return response

    def remove_item(self, access_token: str):
        """ Remove an item from your account.  This will stop a subscription
            fee. """

        self._print_charge_warning(self.ChargeType.REMOVE)
        request = ItemRemoveRequest(access_token)
        self._log_api(request)
        response = self.client.item_remove(request)
        self._log_api(response)
        return response

#    def get_transactions_sync(self, access_token: str, account_id: str, count: int = 500, cursor: str | None = None):
#        """ Get a list of transactions for an account, a cursor will be
#        available in the download. This marks the location of the last
#        transaction you downloaded.   In additional  requests you can provide
#        this cursor to download only newer transactions. """
#
#        result = {'transactions': []}
#
#        while True:
#            options = TransactionsSyncRequestOptions(account_id=account_id, include_original_description=True)
#            request = TransactionsSyncRequest(access_token=access_token, count=count, cursor="" if cursor is None else cursor, options=options)
#            self._log_api(request)
#            response = self.client.transactions_sync(request)
#            self._log_api(response)
#
#            result['transactions'].extend(response['added'])
#
#            cursor = response['next_cursor']
#            result['cursor'] = cursor
#            if not response['has_more']:
#                break
#        return (cursor, result)

    def get_transactions(self, access_token: str, account_ids: str | list[str] | None = None, start_date=None, end_date=date.today()):
        """ Get a list of transactions for an account. """

        if isinstance(account_ids, str):
            account_ids = [account_ids]

        if start_date is None:
            start_date = end_date - timedelta(days=30)

        offset = 0
        total_transactions = None
        responses = []

        while True:
            options = TransactionsGetRequestOptions()
            options.count = 500
            options.offset = offset
            if account_ids is not None:
                options.account_ids = account_ids

            request = TransactionsGetRequest(access_token=access_token, start_date=start_date, end_date=end_date, options=options)
            self._log_api(request)
            response = self.client.transactions_get(request)
            responses.append(response.to_dict())
            self._log_api(response)

            if total_transactions is None:
                total_transactions = response['total_transactions']

            offset += len(response['transactions'])
            if offset >= total_transactions:
                break

        return responses

    def transactions_refresh(self, access_token: str):
        """ Force plaid to refresh transactions cached on plaids servers.
            This call forces plaid to sync with the bank. """

        self._print_charge_warning(self.ChargeType.ONE_TIME, self.PricingFees['refresh'])
        request = TransactionsRefreshRequest(access_token=access_token)
        self._log_api(request)
        response = self.client.transactions_refresh(request)
        self._log_api(response)
        return response

    def investment_refresh(self, access_token: str):
        """ Force plaid to refresh investment transactions cached on plaids
            servers. This call forces plaid to sync with the bank. """

        self._print_charge_warning(self.ChargeType.ONE_TIME, self.PricingFees['refresh'])
        request = InvestmentsRefreshRequest(access_token=access_token)
        self._log_api(request)
        response = self.client.investments_refresh(request)
        self._log_api(response)
        return response

    def get_investment_transactions(self, access_token: str, account_ids: str | list[str] | None = None, start_date=None, end_date=date.today()):
        """ Get a list of investments transactions for an account. """

        if isinstance(account_ids, str):
            account_ids = [account_ids]

        if start_date is None:
            start_date = end_date - timedelta(days=30)

        offset = 0
        total_transactions = None
        responses = []

        while True:
            options = InvestmentsTransactionsGetRequestOptions()
            options.count = 500
            options.offset = offset
            if account_ids is not None:
                options.account_ids = account_ids

            request = InvestmentsTransactionsGetRequest(access_token=access_token, start_date=start_date, end_date=end_date, options=options)
            self._log_api(request)
            response = self.client.investments_transactions_get(request)
            responses.append(response.to_dict())
            self._log_api(response)

            if total_transactions is None:
                total_transactions = response['total_investment_transactions']

            offset += len(response['investment_transactions'])
            if offset >= total_transactions:
                break

        return responses

    def get_investment_holdings(self, access_token: str, account_ids: str | list[str] | None = None):
        """ Get a list of investments holdings for an account. """

        if isinstance(account_ids, str):
            account_ids = [account_ids]

        options = InvestmentHoldingsGetRequestOptions()

        if account_ids is not None:
            options.account_ids = account_ids

        request = InvestmentsHoldingsGetRequest(access_token=access_token, options=options)
        self._log_api(request)
        response = self.client.investments_holdings_get(request)
        self._log_api(response)
        return [response.to_dict()]
