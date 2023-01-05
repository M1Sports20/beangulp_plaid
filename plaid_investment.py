__copyright__ = "Copyright (C) 2022 Michael Spradling"
__license__ = "GNU GPLv2"

import json
import beangulp
from dateutil.parser import parse
from datetime import date, timedelta

from beancount.core import data
from beancount.core import amount
from beancount.core import position
from beancount.core import flags
from beangulp.testing import main
from beancount.core.number import D


class Importer(beangulp.Importer):
    def __init__(self, account_name, account_id):
        self.account_name = account_name
        self.account_id = account_id
        self.cash_acount_name = "Cash"
        self.plaid_func_table = {
                    ("cash", "deposit"): self.cash_deposit,
                    ("fee", "miscellaneous fee"): self.misc_fee
                }

    def identify(self, filepath):
        with open(filepath) as fp:
            try:
                j = json.load(fp)
            except:
                return False

            if 'investment_transactions' in j['response_types']:
                if "accounts" in j['investment_transactions']:
                    if j['investment_transactions']['accounts'][0]['account_id'] == self.account_id:
                        return True
        return False

    def account(self, filepath):
        return self.account_name

    def filename(self, filepath):
        return f"{self.account_name.split(':')[-1]}.json"

    def extract_prices(self, filepath, existing):
        entries = []
        with open(filepath) as fp:
            j = json.load(fp)

            if 'investment_holdings' in j['response_types']:
                securities = j['investment_holdings']['securities']

                for index, security in enumerate(securities):
                    t_date = parse(security['close_price_as_of']).date()
                    t_ticker = security['ticker_symbol']
                    t_price = security['close_price']
                    t_currency = security['iso_currency_code']
                    meta = data.new_metadata(filepath, index)
                    price = data.Price(meta, t_date, t_ticker, amount.Amount(D(str(t_price)), t_currency))
                    entries.append(price)
        return entries

    def cash_deposit(self, meta, t, s):
        t_currency = t['iso_currency_code']
        t_amount = amount.Amount(D(str(t['amount'])), t_currency)
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s_name = s['name']
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)),
                            -t_amount, None, None, None, {'transaction_id': t_transaction_id}))
        postings.append(data.Posting("Unknown Account", t_amount, None, None, None, None))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def misc_fee(self, meta, t, s):
        t_currency = t['iso_currency_code']
        t_amount = amount.Amount(D(str(t['amount'])), t_currency)
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        s_cash_equivalent = s['is_cash_equivalent']
        s_currency = t['iso_currency_code']
        s_price = amount.Amount(D(str(s['close_price'])), s_currency)
        postings = []

        assert s_cash_equivalent is True, "Currently, only sweeps are supported for misc fees"

        meta['transaction_id'] = t_transaction_id
        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)),
                            -t_amount, None, None, None, None))
        cost = position.Cost(D(str(s['close_price'])), s_currency, t_date, None)
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), s_price, cost, s_price, None, None))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def extract(self, filepath, existing):
        entries = []
        with open(filepath) as fp:
            j = json.load(fp)

            if 'investment_transactions' in j['response_types']:
                trans = j['investment_transactions']
                transactions = trans['investment_transactions']
                securities =  trans['securities']
                balance = str(trans['accounts'][0]['balances']['current'])
                currency = trans['accounts'][0]['balances']['iso_currency_code']
                last_date = date.min

                # Build Securities lookup_table
                sec_table = {}
                for s in securities:
                    sec_table[s['security_id']] = s

                # Handle Transactions
                for index, t in enumerate(transactions):
                    t_type = t['type']
                    t_subtype = t['subtype']
                    t_sec_id = t['security_id']
                    meta = data.new_metadata(filepath, index)

                    trans_func = self.plaid_func_table.get((t_type, t_subtype))
                    assert trans_func is not None, f"Unhandled Investment transaction (type: { t_type }, subtype: { t_subtype } )"
                    entries.append(trans_func(meta, t, sec_table[t_sec_id]))

            entries.append(extract_prices(self, filepath, existing)
            return entries


if __name__ == '__main__':
    importer = Importer('Assets:Current:Plaid401k',
                        'Bpa7oJZdJ7u8npdJvo1XCN31433Ga4fL7dk8N')
    main(importer)
