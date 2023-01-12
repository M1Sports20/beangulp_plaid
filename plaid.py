__copyright__ = "Copyright (C) 2022 Michael Spradling"
__license__ = "GNU GPLv2"

import json
import beangulp
from dateutil.parser import parse
from datetime import timedelta

from beancount.core import data
from beancount.core import amount
from beancount.core import position
from beancount.core import flags
from beangulp.testing import main
from beancount.core.number import D, ZERO

DUPLICATE = '__duplicate__'


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
                    ("buy", "buy"): self.__buy_buy_sell_sell,
                    ("sell", "sell"): self.__buy_buy_sell_sell
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
                ent, sec_changed = self.extract_investment_transactions(filepath, j['resp_item'], j['resp_investment_transactions'])
                if len(ent):
                    entries.extend(ent)

                # Build list of commodities for price lookup of unknown tickers
                commodities = {}
                for c in existing:
                    if isinstance(c, data.Commodity):
                        (meta, date, currency) = c
                        commodities[meta['name']] = currency

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

    def deduplicate(self, entries: data.Entries, existing: data.Entries) -> data.Entries:
        marked = []
        plaid_ids = set()

        # Create set of plaid_ids
        for entry in existing:
            if isinstance(entry, data.Transaction):
                for posting in entry.postings:
                    if 'plaid_id' in posting.meta:
                        plaid_ids.add(posting.meta['plaid_id'])

        for entry in entries:
            if isinstance(entry, data.Transaction):
                for posting in entry.postings:
                    if posting.meta is not None and 'plaid_id' in posting.meta:
                        if posting.meta['plaid_id'] in plaid_ids:
                            meta = entry.meta.copy()
                            meta[DUPLICATE] = True
                            entry = entry._replace(meta=meta)
            marked.append(entry)
        return marked

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
            pst_account = ':'.join(t['category']).replace("'", "").replace(',', '').replace(' ', '-')

            # Create beancount transaction with posting
            leg1 = data.Posting(self.account_name, -units, None, None, None, {'plaid_id': plaid_id})
            leg2 = data.Posting("Expenses:" + pst_account, units, None, None, None, None)
            txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, merchant, description, data.EMPTY_SET, data.EMPTY_SET, [leg1, leg2])
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
                price = data.Price(meta, t_date, t_ticker, amount.Amount(D(str(t_price)), t_currency))
                entries.append(price)
        return entries

    def extract_investment_transactions(self, filepath, resp_item, resp_transactions):
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
            ent = trans_func(meta, t, securities)
            if ent is not None:
                entries.append(ent)
                securities_changed.add(t_security_id)

        return (entries, securities_changed)

    def __build_securities_table(self, securities):
        sec_table = {}
        for s in securities:
            sec_table[s['security_id']] = s
        return sec_table

    def __cash_deposit_withdrawal(self, meta, transaction, securities):
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

    def __fee_misc_fee(self, meta, transaction, securities):
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

    def __fee_dividend(self, meta, transaction, securities):
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

    def __buy_buy_sell_sell(self, meta, transaction, securities):
        t_sec_id = transaction['security_id']
        t_currency = transaction['iso_currency_code']
        t_amount = D(str(transaction['amount']))
        t_date = parse(transaction['date']).date()
        t_name = transaction['name']
        t_plaid_id = transaction['investment_transaction_id']
        s = securities[t_sec_id]
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        t_quantity = amount.Amount(D(str(transaction['quantity'])), s_ticker)
        t_price = D(str(transaction['price']))
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_account_name)), -amount.Amount(t_amount, t_currency), None, None, None, None))
        cost = position.Cost(t_price, t_currency, t_date, None)
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), t_quantity, cost, amount.Amount(t_price, t_currency), None, {'plaid_id': t_plaid_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn


if __name__ == '__main__':
    importer = Importer('Assets:Current:PlaidSampleBank',
                        'lba8R6D568uraJgQw6RZfVjRBjjxzBurLjM89')
    main(importer)
