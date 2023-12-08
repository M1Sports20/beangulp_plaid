__copyright__ = "Copyright (C) 2022 Michael Spradling"
__license__ = "GNU GPLv2"

import collections
import json
import beangulp
from dateutil.parser import parse
from datetime import timedelta

from beancount.core import data, amount, position, flags, interpolate
from beangulp.testing import main
from beancount.core.number import D, ZERO, ONE


class Importer(beangulp.Importer):
    def __init__(self, account_name, account_id):
        self.account_name = account_name
        self.account_id = account_id
        self.cash_account_name = "Cash"
        # Plaid defines many times of investment transations by mapping them
        # to a type and subtype.  Here we map type and subtype to a callback
        # function that creates the positing for that transaction type.
        self.plaid_func_table = {
                    ("cash", "deposit"): self.__cash_deposit_withdrawal,
                    ("cash", "withdrawal"): self.__cash_deposit_withdrawal,
                    ("fee", "miscellaneous fee"): self.__fee_misc_fee,
                    ("fee", "interest"): self.__fee_misc_fee,
                    ("fee", "dividend"): self.__fee_dividend,
                    ("cash", "dividend"): self.__fee_dividend,
                    ("buy", "buy"): self.__buy_buy_sell_sell,
                    ("sell", "sell"): self.__buy_buy_sell_sell,
                    ("cash", "contribution"): self.__buy_buy_sell_sell,
                    ("transfer", "transfer"): self.__cash_transfer
        }

    def identify(self, filepath):
        with open(filepath) as fp:
            try:
                j = json.load(fp)
            except:
                return False

            if 'resp_transactions' in j:
                account = j['resp_transactions']['accounts'][0]
                if self.account_id == account['account_id']:
                    return True
            elif 'resp_investment_transactions' in j:
                account = j['resp_investment_transactions']['accounts'][0]
                if self.account_id == account['account_id']:
                    return True
        return False

    def account(self, filepath):
        return self.account_name

    def filename(self, filepath):
        return f"{self.account_name.split(':')[-1]}.json"

    def extract(self, filepath, existing):
        entries = []
        with open(filepath) as fp:
            j = json.load(fp)
            if 'resp_transactions' in j:
                ent = self.extract_transactions(filepath, j['resp_item'], j['resp_transactions'])
                if len(ent):
                    entries.extend(ent)

            if 'resp_investment_transactions' in j:
                # Build list of commodities for price lookup of unknown tickers
                commodities = {}
                for c in existing:
                    if isinstance(c, data.Commodity):
                        (meta, date, currency) = c
                        commodities[meta['name']] = currency
                        if 'name2' in meta:
                            commodities[meta['name2']] = currency

                ent, sec_changed = self.extract_investment_transactions(filepath, j['resp_item'], j['resp_investment_transactions'], commodities)
                if len(ent):
                    entries.extend(ent)

                if 'resp_investment_holdings' in j:
                    # Balance asserts
                    resp_holdings = j['resp_investment_holdings']
                    ent = self.create_sec_balance_asserts(filepath, sec_changed, resp_holdings, commodities)
                    if len(ent):
                        entries.extend(ent)

                    # Extract prices
                    ent = self.extract_prices(filepath, resp_holdings, commodities)
                    if len(ent):
                        entries.extend(ent)

        return entries

    def create_sec_balance_asserts(self, filepath, securities, resp_holdings, commodities):
        entries = []
        holdings_table = self.__build_securities_table(resp_holdings['holdings'])
        securities_table = self.__build_securities_table(resp_holdings['securities'])

        for index, security_id in enumerate(securities):
            if security_id in holdings_table:
                holding = holdings_table[security_id]
                t_quantity = str(holding['quantity'])
                t_date = parse(holding['institution_price_as_of']).date()
                security = securities_table[security_id]
                t_ticker = security['ticker_symbol']
                s_name = security['name']
                # Unknown ticker. Use name to lookup commodity from ledger
                if t_ticker is None and s_name in commodities:
                    t_ticker = commodities[s_name]

                meta = data.new_metadata(filepath, index)
                amt = amount.Amount(D(t_quantity), t_ticker)
                entries.append(data.Balance(meta, t_date + timedelta(days=1), ":".join((self.account_name, t_ticker)), amt, None, None))
            else:
                pass
                # TODO: We had a transaction on a holding we no longer hold. balance should be 0

        return entries

    def extract_transactions(self, filepath, resp_item, resp_transactions):
        entries = []
        account = resp_transactions['accounts'][0]
        balance = str(account['balances']['current'])
        available_bal = str(account['balances']['available'])  # Venmo, and some others don't have balance, but available balance only
        if balance == "None":
            balance = available_bal
        acct_type = account['type']
        currency = account['balances']['iso_currency_code']
        transactions = resp_transactions['transactions']
        transactions_date = parse(resp_item['status']['transactions']['last_successful_update']).date()

        for index, t in enumerate(transactions):
            # Ignore pending transactions
            if t['pending'] is True:
                continue

            # Extract fields from transaction
            t_date = parse(t['date']).date()
            merchant = t['merchant_name']
            description = t['name']
            amt = str(t['amount'])
            plaid_id = t['transaction_id']
            meta = data.new_metadata(filepath, index)
            units = amount.Amount(D(amt), currency)

            # Create beancount transaction with posting
            leg1 = data.Posting(self.account_name, -units, None, None, None, {'plaid_id': plaid_id})
            txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, merchant, description, data.EMPTY_SET, data.EMPTY_SET, [leg1])
            entries.append(txn)

        # Insert a final balance check
        if len(entries):
            meta = data.new_metadata(filepath, 0)
            amt = amount.Amount(D(balance), currency)
            amt = -amt if acct_type == "credit" or acct_type == "loan" else amt
            entries.append(data.Balance(meta, transactions_date + timedelta(days=1), self.account_name, amt, None, None))
        return entries

    def extract_prices(self, filepath, resp_holdings, commodities):
        entries = []
        securities = self.__build_securities_table(resp_holdings['securities'])

        for index, holding in enumerate(resp_holdings['holdings']):
            t_price = holding['institution_price']
            t_date = parse(holding['institution_price_as_of']).date()
            t_currency = holding['iso_currency_code']
            security = securities[holding['security_id']]
            t_ticker = security['ticker_symbol']
            s_name = security['name']
            meta = data.new_metadata(filepath, index)

            # Unknown ticker. Use name to lookup commodity from ledger
            if t_price != ZERO:
                if t_ticker is None and s_name in commodities:
                    t_ticker = commodities[s_name]
                assert t_ticker is not None, f"Ticker unknown and couldn't lookup by name in ledger by name: ({ s_name })"
                price = data.Price(meta, t_date - timedelta(days=1), t_ticker, amount.Amount(D(str(t_price)), t_currency))
                entries.append(price)
        return entries

    def extract_investment_transactions(self, filepath, resp_item, resp_transactions, commodities):
        securities_changed = set()
        entries = []
        transactions = resp_transactions['investment_transactions']
        securities = self.__build_securities_table(resp_transactions['securities'])

        # Handle Transactions
        for index, t in enumerate(transactions):
            t_type = t['type']
            t_security_id = t['security_id']
            t_subtype = t['subtype']
            trans_func = self.plaid_func_table.get((t_type, t_subtype))
            assert trans_func is not None, f"Unhandled Investment transaction (type: { t_type }, subtype: { t_subtype } )"
            meta = data.new_metadata(filepath, index)
            ent = trans_func(meta, t, securities, commodities)
            if ent is not None:
                entries.append(ent)
                securities_changed.add(t_security_id)

        return (entries, securities_changed)

    def __build_securities_table(self, securities):
        sec_table = {}
        for s in securities:
            sec_table[s['security_id']] = s
        return sec_table

    def __cash_deposit_withdrawal(self, meta, transaction, securities, commodities):
        if transaction['quantity'] is not None:
            return self.__buy_buy_sell_sell(meta, transaction, securities, commodities)

        t_sec_id = transaction['security_id']
        t_currency = transaction['iso_currency_code']
        t_amount = amount.Amount(D(str(transaction['amount'])), t_currency)
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        s = securities[t_sec_id]
        s_name = s['name']
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -t_amount, None, None, None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __cash_transfer(self, meta, transaction, securities, commodities):
        t_currency = transaction['iso_currency_code']
        t_amount = amount.Amount(D(str(transaction['amount'])), t_currency)
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -t_amount, None, None, None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, t_currency, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __fee_misc_fee(self, meta, transaction, securities, commodities):
        t_sec_id = transaction['security_id']
        t_currency = transaction['iso_currency_code']
        t_amount = amount.Amount(D(str(transaction['amount'])), t_currency)
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        s = securities[t_sec_id]
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        s_cash_equivalent = s['is_cash_equivalent']
        s_amount = amount.Amount(D(str(transaction['amount'])), s_ticker)
        s_currency = s['iso_currency_code']
        s_price = amount.Amount(D(str(s['close_price'])), s_currency)  # only works because we only support cash_equivalent
        postings = []
        assert s_cash_equivalent is True, "Currently, only sweeps are supported for misc fees"

        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -t_amount, None, None, None, None))
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), s_amount, None, s_price, None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __fee_dividend(self, meta, transaction, securities, commodities):
        t_sec_id = transaction['security_id']
        t_currency = transaction['iso_currency_code']
        t_amount = amount.Amount(D(str(transaction['amount'])), t_currency)
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        s = securities[t_sec_id]
        s_name = s['name']
        postings = []

        postings.append(data.Posting("Income:Dividend", t_amount, None, None, None, None))
        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -t_amount, None, None, None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __buy_buy_sell_sell(self, meta, transaction, securities, commodities):
        t_sec_id = transaction['security_id']
        t_currency = transaction['iso_currency_code']
        t_amount = D(str(transaction['amount']))
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        s = securities[t_sec_id]
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        if s_ticker is None and s_name in commodities:
            s_ticker = commodities[s_name]
        s_name = s['name']
        t_quantity = amount.Amount(D(str(transaction['quantity'])), s_ticker)
        t_price = D(str(transaction['price']))
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -amount.Amount(t_amount, t_currency), None, None, None, None))
        cost = position.Cost(t_price, t_currency, t_date, None)
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), t_quantity, cost, amount.Amount(t_price, t_currency), None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn


class PlaidSimilarityComparator:
    """Similarity comparator of imported Plaid transactions.

    This comparator needs to be able to handle Transaction instances which are
    incomplete on one side, which have slightly different dates, or potentially
    slightly different numbers.
    """

    # Fraction difference allowed of variation.
    EPSILON = D('0.05')  # 5%

    def __init__(self, max_date_delta=None):
        """Constructor a comparator of entries.
        Args:
          max_date_delta: A datetime.timedelta instance of the max tolerated
            distance between dates.
        """
        self.cache = {}
        self.max_date_delta = max_date_delta

    def __call__(self, entry1, entry2):
        """Compare two entries, return true if they are deemed similar.

        Args:
          entry1: A first Transaction directive.
          entry2: A second Transaction directive.
        Returns:
          A boolean.
        """
        # Check the date difference.
        if self.max_date_delta is not None:
            delta = ((entry1.date - entry2.date)
                     if entry1.date > entry2.date else
                     (entry2.date - entry1.date))
            if delta > self.max_date_delta:
                return False

        try:
            amounts1 = self.cache[id(entry1)]
        except KeyError:
            amounts1 = self.cache[id(entry1)] = amounts_map(entry1)
        try:
            amounts2 = self.cache[id(entry2)]
        except KeyError:
            amounts2 = self.cache[id(entry2)] = amounts_map(entry2)

        # Look for amounts on common accounts.
        common_keys = set(amounts1) & set(amounts2)
        for key in sorted(common_keys):
            # Compare the amounts.
            number1 = amounts1[key]
            number2 = amounts2[key]
            if number1 == ZERO and number2 == ZERO:
                break
            diff = abs((number1 / number2)
                       if number2 != ZERO
                       else (number2 / number1))
            if diff == ZERO:
                return False
            if diff < ONE:
                diff = ONE/diff
            if (diff - ONE) < self.EPSILON:
                break
        else:
            return False

        return True


def amounts_map(entry):
    """Compute a mapping of (account, currency) -> Decimal balances.

    Args:
      entry: A Transaction instance.
    Returns:
      A dict of account -> Amount balance.
    """
    amounts = collections.defaultdict(D)
    for posting in entry.postings:
        if not posting.meta:
            continue
        # Skip interpolated postings.
        if interpolate.AUTOMATIC_META in posting.meta or 'plaid_id' not in posting.meta:
            continue
        currency = isinstance(posting.units, amount.Amount) and posting.units.currency
        if isinstance(currency, str):
            plaid_id = posting.meta['plaid_id'] if 'plaid_id' in posting.meta else None
            key = (posting.account, plaid_id, currency)
            amounts[key] += posting.units.number
    return amounts


if __name__ == '__main__':
    importer = Importer('Assets:Current:PlaidSampleBank',
                        'lba8R6D568uraJgQw6RZfVjRBjjxzBurLjM89')
    main(importer)
