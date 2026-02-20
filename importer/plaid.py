__copyright__ = "Copyright (C) 2022-2026  Michael Spradling"
__license__ = "GNU GPLv2"

import beangulp
import json
import logging

from beancount.core import amount, data, flags, position
from beangulp.testing import main
from os import path
from dateutil.parser import parse
from datetime import timedelta, date
from beancount.core.number import D, ZERO, ONE

logging.basicConfig(level=logging.WARN)

# TODO: move to create_simple_posting and create_simple_posting_with_cost

# TODO
#id_cmp = similar.same_link_comparator(regex=r"^plaid_id:\d+$")
#def same_link_comparator(regex: Optional[str] = None) -> Comparator:
#    """Comparison function generator that checks if two directives share a link.
#
#    You can use this if you have a source of transactions that consistently
#    defined and unique transaction which it produces as a link. The matching of
#    these transactions will be more precise than the heuristic no dates and
#    amounts used by default, if you keep the links in your ledger.
#
#    You can further restrict the set of links that are compared if you provide a
#    regex. This can be useful if your importer produces multiple other links.
#
#    Args:
#      regex: An optional regular expression used to filter the links.
#    Returns:
#      A comparator predicate accepting two directives and returning a bool.
#    """
#
#    def cmp(entry1: data.Directive, entry2: data.Directive) -> bool:
#        """Compare two entries by common link."""
#
#        if not isinstance(entry1, data.Transaction) or not isinstance(
#            entry2, data.Transaction
#        ):
#            return False
#
#        links1 = entry1.links
#        links2 = entry2.links
#        if regex:
#            links1 = {link for link in links1 if re.match(regex, link)}
#            links2 = {link for link in links2 if re.match(regex, link)}
#
#        return bool(links1 & links2)
#
#    return cmp


class Importer(beangulp.Importer):
    #cmp = staticmethod(id_cmp)

    def __init__(self, account_name, account_id, cash_account=None,
                 dividend_income_account=None, fees_account=None,
                 gains_loss_account=None, exclude_descriptions=None,
                 rounding_account=None, money_market_funds=None,
                 balance_timedelta=timedelta(days=1)):
        #  account_name: An account string, the account onto which to post all the
        #      amounts parsed.
        #  account_id: Acount id to import
        #  cash_account: Account to move cash to/from
        #  dividend_income_account: Account to book dividend income from
        #  fees_account: Expense account to book account fees to
        #  gain_loss_account: Income account to book Profit/Loss (capital
        #      gains/loss) to/from
        #  exclude_descriptions: List of strings in description to ignore
        #      during import.  Used for accounts that purchase/sell into a
        #      holding fund. 
        #  rounding_account: Account to book rounding errors to. The default,
        #      None, disables this feature
        #  money_market_funds: List of money market funds of which to not
        #      track cost basis
        self.account_name = account_name  # An account string, the account onto which to post all the amounts parsed.
        self.account_id = account_id      # The plaid account id to parse transactions for
        self.cash_account = cash_account or f'{account_name}:Cash'
        self.dividend_income_account = dividend_income_account or 'Income:Dividends'
        self.fees_account = fees_account or 'Expenses:Fees'
        self.gains_loss_account = gains_loss_account or 'Income:PnL'
        self.exclude_descriptions = [exclude_descriptions] if isinstance(exclude_descriptions, str) else exclude_descriptions
        self.rounding_account = rounding_account or 'Equity:Rounding-Error'
        if money_market_funds is None:
            money_market_funds = ''
        self.money_market_funds = [money_market_funds] if isinstance(money_market_funds, str) else money_market_funds
        self.balance_timedelta = balance_timedelta

        # The following structure associates a transaction plaid transaction(type, subtype)
        # to a callback to handle that transaction.
        self.inv_trans_cb = {
            'buy': {
                'assignment': self.unknown_transaction,               # 'Assignment of short option holding'
                'contribution': self.unknown_transaction,             # 'Inflow of assets into a tax-advantaged account'
                'buy': self.invs_buy_sell,                            # 'Purchase to open or increase a position'
                'buy to cover': self.unknown_transaction,             # 'Purchase to close a short position'
                'dividend reinvestment': self.unknown_transaction,    # 'Purchase using proceeds from a cash dividend'
                'interest reinvestment': self.unknown_transaction,    # 'Purchase using proceeds from a cash interest payment'
                'long-term capital gain reinvestment': self.invs_buy_reinvestment,   # 'Purchase using long-term capital gain cash proceeds'
                'short-term capital gain reinvestment': self.unknown_transaction,  # 'Purchase using short-term capital gain cash proceeds'
            },
            'sell': {
                'distribution': self.unknown_transaction,             # 'Outflow of assets from a tax-advantaged account'
                'exercise': self.unknown_transaction,                 # 'Exercise of an option or warrant contract'
                'sell': self.invs_buy_sell,                           # 'Sell to close or decrease an existing holding'
                'sell short': self.unknown_transaction,               # 'Sell to open a short position'
            },
            'cancel': {None: self.unknown_transaction},               # 'A cancellation of a pending transaction'
            'cash': {
                'account fee': self.unknown_transaction,              # 'Fees paid for account maintenance'
                'contribution': self.invs_cash_deposit,               # 'Inflow of assets into a tax-advantaged account'
                'deposit': self.invs_cash_deposit,                    # 'Inflow of cash into an account'
                'dividend': self.invs_cash_dividend,                  # 'Inflow of cash from a dividend'
                'stock distribution': self.unknown_transaction,       # 'Inflow of stock from a distribution'
                'interest': self.invs_cash_dividend,                  # 'Inflow of cash from interest'
                'legal fee': self.unknown_transaction,                # 'Fees paid for legal charges or services'
                'long-term capital gain': self.unknown_transaction,   # 'Long-term capital gain received as cash'
                'management fee': self.unknown_transaction,           # 'Fees paid for investment management of a mutual fund or other pooled investment vehicle'
                'margin expense': self.unknown_transaction,           # 'Fees paid for maintaining margin debt'
                'non-qualified dividend': self.unknown_transaction,   # 'Inflow of cash from a non-qualified dividend'
                'non-resident tax': self.unknown_transaction,         # 'Taxes paid on behalf of the investor for non-residency in investment jurisdiction'
                'pending credit': self.unknown_transaction,           # 'Pending inflow of cash'
                'pending debit': self.unknown_transaction,            # 'Pending outflow of cash'
                'qualified dividend': self.unknown_transaction,       # 'Inflow of cash from a qualified dividend'
                'short-term capital gain': self.unknown_transaction,  # 'Short-term capital gain received as cash'
                'tax': self.unknown_transaction,                      # 'Taxes paid on behalf of the investor'
                'tax withheld': self.unknown_transaction,             # 'Taxes withheld on behalf of the customer'
                'transfer fee': self.unknown_transaction,             # 'Fees incurred for transfer of a holding or account'
                'trust fee': self.unknown_transaction,                # 'Fees related to administration of a trust account'
                'unqualified gain': self.unknown_transaction,         # 'Unqualified capital gain received as cash'
                'withdrawal': self.invs_cash_withdrawal,              # 'Outflow of cash from an account'
            },
            'fee': {
                'account fee': self.invs_fees_account,                # 'Fees paid for account maintenance'
                'adjustment': self.unknown_transaction,               # 'Increase or decrease in quantity of item'
                'dividend': self.unknown_transaction,                 # 'Inflow of cash from a dividend'
                'interest': self.unknown_transaction,                 # 'Inflow of cash from interest'
                'interest receivable': self.unknown_transaction,      # 'Inflow of cash from interest receivable'
                'long-term capital gain': self.unknown_transaction,   # 'Long-term capital gain received as cash'
                'legal fee': self.unknown_transaction,                # 'Fees paid for legal charges or services'
                'management fee': self.unknown_transaction,           # 'Fees paid for investment management of a mutual fund or other pooled investment vehicle'
                'margin expense': self.unknown_transaction,           # 'Fees paid for maintaining margin debt'
                'miscellaneous fee': self.invs_fees_account,          # UNDOCUMENTED, but fidelity has this
                'non-qualified dividend': self.unknown_transaction,   # 'Inflow of cash from a non-qualified dividend'
                'non-resident tax': self.unknown_transaction,         # 'Taxes paid on behalf of the investor for non-residency in investment jurisdiction'
                'qualified dividend': self.unknown_transaction,       # 'Inflow of cash from a qualified dividend'
                'return of principal': self.unknown_transaction,      # 'Repayment of loan principal'
                'short-term capital gain': self.unknown_transaction,  # 'Short-term capital gain received as cash'
                'stock distribution': self.unknown_transaction,       # 'Inflow of stock from a distribution'
                'tax': self.unknown_transaction,                      # 'Taxes paid on behalf of the investor'
                'tax withheld': self.unknown_transaction,             # 'Taxes withheld on behalf of the customer'
                'transfer fee': self.unknown_transaction,             # 'Fees incurred for transfer of a holding or account'
                'trust fee': self.unknown_transaction,                # 'Fees related to administration of a trust account'
                'unqualified gain': self.unknown_transaction,         # 'Unqualified capital gain received as cash'
            },
            'transfer': {
                'assignment': self.unknown_transaction,               # 'Assignment of short option holding'
                'adjustment': self.unknown_transaction,               # 'Increase or decrease in quantity of item'
                'exercise': self.unknown_transaction,                 # 'Exercise of an option or warrant contract'
                'expire': self.unknown_transaction,                   # 'Expiration of an option or warrant contract'
                'merger': self.unknown_transaction,                   # 'Stock exchanged at a pre-defined ratio as part of a merger between companies'
                'request': self.unknown_transaction,                  # 'Request fiat or cryptocurrency to an address or email'
                'send': self.unknown_transaction,                     # 'Inflow or outflow of fiat or cryptocurrency to an address or email'
                'spin off': self.unknown_transaction,                 # 'Inflow of stock from spin-off transaction of an existing holding'
                'split': self.unknown_transaction,                    # 'Inflow of stock from a forward split of an existing holding'
                'trade': self.unknown_transaction,                    # 'Trade of one cryptocurrency for another'
                'transfer': self.unknown_transaction,                 # 'Movement of assets into or out of an account'
            },
        }

    def unknown_transaction(self, transaction, filepath, index):
        _type = transaction['type']
        subtype = transaction['subtype']
        plaid_id = transaction['investment_transaction_id']
        raise NotImplementedError(f"Id: {plaid_id} investment transaction not supported, type: {_type}, Subtype: {subtype}")

    def invs_cash_withdrawal(self, transaction, filepath, index):
        postings = []

        amt = D(str(transaction['amount']))
        currency = transaction['iso_currency_code']
        date = parse(transaction['date']).date()
        name = transaction['name']
        post_meta = {'plaid_id': transaction['investment_transaction_id']}

        post = data.Posting(self.cash_account, -amount.Amount(amt, currency), None, None, None, post_meta)
        postings.append(post)

        meta = data.new_metadata(filepath, index)
        txn = data.Transaction(meta, date, flags.FLAG_OKAY, None, name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def invs_cash_deposit(self, transaction, filepath, index):
        postings = []

        amt = D(str(transaction['amount']))
        currency = transaction['iso_currency_code']
        date = parse(transaction['date']).date()
        name = transaction['name']
        post_meta = {'plaid_id': transaction['investment_transaction_id']}

        post = data.Posting(self.cash_account, -amount.Amount(amt, currency), None, None, None, post_meta)
        postings.append(post)

        meta = data.new_metadata(filepath, index)
        txn = data.Transaction(meta, date, flags.FLAG_OKAY, None, name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def invs_cash_dividend(self, transaction, filepath, index):
        postings = []

        amt = D(str(transaction['amount']))
        currency = transaction['iso_currency_code']
        date = parse(transaction['date']).date()
        name = transaction['name']
        security = self.securities[transaction['security_id']]
        security_name = security['name']
        post_meta = {'plaid_id': transaction['investment_transaction_id']}

        # Post: Acount to move cash from
        post = data.Posting(self.dividend_income_account, amount.Amount(amt, currency), None, None, None, post_meta)
        postings.append(post)

        post = data.Posting(self.cash_account, -amount.Amount(amt, currency), None, None, None, None)
        postings.append(post)

        meta = data.new_metadata(filepath, index)
        txn = data.Transaction(meta, date, flags.FLAG_OKAY, security_name, name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def invs_buy_reinvestment(self, transaction, filepath, index):
        plaid_id = transaction['investment_transaction_id']
        return self._invs_buy(self.dividend_income_account, transaction, filepath, index)

    def invs_buy_sell(self, transaction, filepath, index):
        return self._invs_buy(self.cash_account, transaction, filepath, index)

    def invs_fees_account(self, transaction, filepath, index):
        security = self.securities[transaction['security_id']]
        postings = []
        if security['ticker_symbol'] is None and security['is_cash_equivalent']:
            amt = D(str(transaction['amount']))
            currency = transaction['iso_currency_code']
            date = parse(transaction['date']).date()
            post_meta = {'plaid_id': transaction['investment_transaction_id']}
            name = transaction['name']

            post = data.Posting(self.fees_account, amount.Amount(amt, currency), None, None, None, None)
            postings.append(post)
            post = data.Posting(self.cash_account, -amount.Amount(amt, currency), None, None, None, post_meta)
            postings.append(post)

            meta = data.new_metadata(filepath, index)
            txn = data.Transaction(meta, date, flags.FLAG_OKAY, None, name, data.EMPTY_SET, data.EMPTY_SET, postings)
            return txn

        else:
            return self._invs_buy(self.fees_account, transaction, filepath, index)

    def _invs_buy(self, account, transaction, filepath, index):
        security = self.securities[transaction['security_id']]
        date = parse(transaction['date']).date()
        name = transaction['name']
        if security['ticker_symbol'] is not None:
            security_ticker = security['ticker_symbol']
        else:
            security_ticker = "unknown"
        security_name = security['name']
        quantity = D(str(transaction['quantity']))
        amt = D(str(transaction['amount']))
        price = D(str(transaction['price']))
        fees = D(str(transaction['fees']))
        currency = transaction['iso_currency_code']
        _type = transaction['type']
        post_meta = {'plaid_id': transaction['investment_transaction_id']}

        postings = []

        # Post: Acount to move cash from
        post = data.Posting(account, -amount.Amount(amt + fees, currency), None, None, None, None)
        postings.append(post)

        # Post: Fees
        if fees != ZERO:
            post = data.Posting(self.fees_account, amount.Amount(fees, currency), None, None, None, None)
            postings.append(post)

        # Is this a tracking cash mutal_fund
        if security_ticker in self.money_market_funds:
            logging.debug(f"Found mutual fund that tracks the dollar({security_ticker}")
            post = data.Posting(f"{self.account_name}:{security_ticker}", amount.Amount(amt + fees, security_ticker), None, amount.Amount(ONE, 'USD'), None, post_meta)
            postings.append(post)
        else:
            # Post: To security
            cost = position.Cost(price, currency, date, None)
            post = data.Posting(f"{self.account_name}:{security_ticker}", amount.Amount(quantity, security_ticker), cost, amount.Amount(amt, currency), None, post_meta)
            postings.append(post)

            gain_loss_or_rounding_error = amt - quantity * price

            if gain_loss_or_rounding_error != ZERO:
                if _type == "sell":
                    # Post: Profit/Gain Loss when selling
                    post = data.Posting(self.gains_loss_account, amount.Amount(gain_loss_or_rounding_error, currency), None, None, None, None)
                    postings.append(post)
                else:
                    # Post: Rounding error when buying
                    post = data.Posting(self.rounding_account, amount.Amount(gain_loss_or_rounding_error, currency), None, None, None, None)
                    postings.append(post)

        meta = data.new_metadata(filepath, index)
        txn = data.Transaction(meta, date, flags.FLAG_OKAY, security_name, name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def identify(self, filepath):
        logging.debug(f"identify({filepath})")
        # Valid Json file
        with open(filepath) as fd:
            try:
                j = json.load(fd)
            except:
                return False

        # Look for a valid account id in any type of downloaded call
        for field in ['transactions', 'investment_transactions', 'investment_holdings']:
            if field in j:
                for item in j[field]:
                    if 'accounts' in item:
                        accounts = item['accounts']
                        for account in accounts:
                            if self.account_id == account['account_id']:
                                return True

        return False

    def filename(self, filepath):
        logging.debug(f"filename({filepath})")
        if self.account_name:
            return self.account_name + path.splitext(filepath)[1]

    def account(self, filepath):
        logging.debug(f"account({filepath})")
        return self.account_name

    def get_balance(self, j):
        for account in j['accounts']:
            if self.account_id == account['account_id']:
                return account['balances']['current']

    def get_currency(self, j):
        logging.debug(f"get_currency({j})")
        for account in j:
            if self.account_id == account['account_id']:
                return account['balances']['iso_currency_code']

    def enumerate(self, json_file, resp_field, field = None):
        field = resp_field if field is None else field
        logging.debug(f"enumerate({resp_field}, {field})")
        if resp_field in json_file:
            for f in json_file[resp_field]:
                for index, item in enumerate(f[field]):
                    if 'account_id' in item and self.account_id != item['account_id']:
                        continue
                    if 'pending' in item and item['pending']:
                        continue
                    yield index, item

    def _extract_bank(self, filepath, existing):
        logging.debug(f"extract_bank({filepath}, {existing})")
        entries = []
        with open(filepath) as fd:
            json_file = json.load(fd)

        # Extract bank transactions
        for index, transaction in self.enumerate(json_file, 'transactions'):
            amt = str(transaction['amount'])
            currency = transaction['iso_currency_code']
            date = parse(transaction['date']).date()
            description = transaction['name']
            merchant = transaction['merchant_name']
            meta = data.new_metadata(filepath, index)
            plaid_id = transaction['transaction_id']
            units = -amount.Amount(D(amt), currency)
            logging.info(plaid_id)

            # Extract transactions
            leg = data.Posting(self.account_name, units, None, None, None,
                               {'plaid_id': plaid_id})
            txn = data.Transaction(meta, date, flags.FLAG_OKAY, merchant, description,
                                   data.EMPTY_SET, data.EMPTY_SET, [leg])

            # Filter out excluded descriptions
            if any(txt in description for txt in self.exclude_descriptions):
                continue

            entries.append(txn)

        return entries

    def _extract_investments(self, filepath, existing):
        logging.debug(f"extract_investment({filepath}, {existing})")
        entries = []
        with open(filepath) as fd:
            json_file = json.load(fd)

        # Extract investment type transactions
        for index, transaction in self.enumerate(json_file, 'investment_transactions'):
            _type = transaction['type']
            subtype = transaction['subtype']

            # Call txn handler
            try:
                cb = self.inv_trans_cb[_type][subtype]
            except KeyError:
                raise NotImplementedError(f"Unknown Transaction Type - {_type}:{subtype} ")
            txn = cb(transaction, filepath, index)

            if txn is not None:
                # Filter out excluded descriptions
                if any(txt in txn.narration for txt in self.exclude_descriptions):
                    continue

                entries.append(txn)

        return entries

    def _get_securities(self, json_file):
        securities = {}
        for index, security in self.enumerate(json_file, 'investment_holdings', 'securities'):
            id = security['security_id']
            securities[id] = security

        for index, security in self.enumerate(json_file, 'investment_transactions', 'securities'):
            id = security['security_id']
            securities[id] = security

        return securities

    def _get_holdings(self, json_file):
        # Generate a dictionary of securities to enable lookup

        holdings = {}
        for index, holding in self.enumerate(json_file, 'investment_holdings', 'holdings'):
            if holding['account_id'] == self.account_id:
                security_id = holding['security_id']
                holdings[security_id] = holding

        return holdings

    def _investment_create_bals(self, filepath, existing):
        # Create price entries from balances
        logging.debug(f"_investment_price_create({filepath}, {existing})")
        entries = []

        with open(filepath) as fd:
            json_file = json.load(fd)

        for index, holding in self.enumerate(json_file, 'investment_holdings', 'holdings'):
            if holding['account_id'] == self.account_id:
                meta = data.new_metadata(filepath, index)
                security_id = holding['security_id']
                security = self.securities[security_id]
                t_date = parse(holding['institution_price_as_of']).date()
                ticker = security['ticker_symbol']
                quantity = D(str(holding['quantity']))
                amt = amount.Amount(quantity, ticker)

                # Create balance checkpoint
                balance = data.Balance(meta, t_date + self.balance_timedelta, f"{self.account_name}:{ticker}", amt, None, None)
                entries.append(balance)

        return entries


    def _investment_create_prices(self, filepath, existing):
        # Create price entries from holdings

        logging.debug(f"_investment_create_prices({filepath}, {existing})")
        entries = []

        with open(filepath) as fd:
            json_file = json.load(fd)

        for index, holding in self.enumerate(json_file, 'investment_holdings', 'holdings'):
            if holding['account_id'] == self.account_id:
                meta = data.new_metadata(filepath, index)
                security_id = holding['security_id']
                security = self.securities[security_id]
                ticker = security['ticker_symbol']

                h_date = parse(holding['institution_price_as_of']).date() if holding['institution_price_as_of'] is not None else date(1970, 1, 1)
                s_date = parse(security['close_price_as_of']).date() if security['close_price_as_of'] is not None else date(1970, 1, 1)
                if s_date > h_date:
                    price = D(str(security['close_price']))
                    currency = security['iso_currency_code']
                    latest_date = s_date
                else:
                    price = D(str(holding['institution_price']))
                    currency = holding['iso_currency_code']
                    latest_date = h_date

                # Create price
                if price != ONE and latest_date > date(1971, 1, 2):
                    p = data.Price(meta, latest_date, ticker, amount.Amount(price, currency))
                    entries.append(p)

        return entries


    def extract(self, filepath, existing):
        logging.debug(f"extract({filepath}, {existing})")
        entries = []

        with open(filepath) as fd:
            json_file = json.load(fd)

        self.securities = self._get_securities(json_file)
        self.holdings = self._get_holdings(json_file)

        entries += self._extract_bank(filepath, existing)
        entries += self._extract_investments(filepath, existing)
        entries += self._investment_create_bals(filepath, existing)
        entries += self._investment_create_prices(filepath, existing)

        return entries

if __name__ == "__main__":
    importer = Importer("Assets:Current:Plaid:Checking", "masdhg6dERoKc1Z4A")
    main(importer)
