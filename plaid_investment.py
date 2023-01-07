__copyright__ = "Copyright (C) 2022 Michael Spradling"
__license__ = "GNU GPLv2"

import json
import beangulp
from dateutil.parser import parse

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
        self.cash_acount_name = "Cash"
        self.plaid_func_table = {
                    ("cash", "deposit"): self.__cash_deposit_withdrawal,
                    ("cash", "withdrawal"): self.__cash_deposit_withdrawal,
                    ("fee", "miscellaneous fee"): self.__fee_misc_fee,
                    ("fee", "interest"): self.__fee_misc_fee,
                    ("fee", "dividend"): self.__fee_dividend,
                    ("buy", "buy"): self.__buy_buy,
                    ("sell", "sell"): self.__buy_buy
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

    def __build_security_table(self, securities):
        sec_table = {}
        for s in securities:
            sec_table[s['security_id']] = s
        return sec_table

    def __build_commodity_table(self, existing):
        self.commodities = {}
        for c in existing:
            if isinstance(c, data.Commodity):
                (meta, date, currency) = c
                self.commodities[meta['name']] = currency

    def __extract_prices(self, filepath, existing, holdings):
        entries = []

        for index, holding in enumerate(holdings):
            t_price = holding['institution_price']
            t_date = parse(holding['institution_price_as_of']).date()
            t_currency = holding['iso_currency_code']
            security = self.holding_securities[holding['security_id']]
            t_ticker = security['ticker_symbol']
            s_name = security['name']
            meta = data.new_metadata(filepath, index)
            # Unknown ticker. Use name to lookup commodity from ledger

            if t_price != ZERO:
                if t_ticker is None and s_name in self.commodities:
                    t_ticker = self.commodities[s_name]
                assert t_ticker is not None, f"Ticker unknown and couldn't lookup by name in ledger by name: ({ s_name })"
                price = data.Price(meta, t_date, t_ticker, amount.Amount(D(str(t_price)), t_currency))
                entries.append(price)
        return entries

    def __cash_deposit_withdrawal(self, meta, t):
        t_sec_id = t['security_id']
        t_currency = t['iso_currency_code']
        t_amount = amount.Amount(D(str(t['amount'])), t_currency)
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s = self.transaction_securities[t_sec_id]
        s_name = s['name']
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)), -t_amount, None, None, None, {'transaction_id': t_transaction_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __fee_misc_fee(self, meta, t):
        t_sec_id = t['security_id']
        t_currency = t['iso_currency_code']
        t_amount = amount.Amount(D(str(t['amount'])), t_currency)
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s = self.transaction_securities[t_sec_id]
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        s_cash_equivalent = s['is_cash_equivalent']
        s_amount = amount.Amount(D(str(t['amount'])), s_ticker)
        s_currency = s['iso_currency_code']
        s_price = amount.Amount(D(str(s['close_price'])), s_currency)  # only works because we only support cash_equivalent
        postings = []

        assert s_cash_equivalent is True, "Currently, only sweeps are supported for misc fees"

        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)), -t_amount, None, None, None, None))
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), s_amount, None, s_price, None, {'transaction_id': t_transaction_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __fee_dividend(self, meta, t):
        t_sec_id = t['security_id']
        t_currency = t['iso_currency_code']
        t_amount = amount.Amount(D(str(t['amount'])), t_currency)
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s = self.transaction_securities[t_sec_id]
        s_name = s['name']
        postings = []

        postings.append(data.Posting("Income:Dividend", t_amount, None, None, None, None))
        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)), -t_amount, None, None, None, {'transaction_id': t_transaction_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def __buy_buy(self, meta, t):
        t_sec_id = t['security_id']
        t_currency = t['iso_currency_code']
        t_amount = D(str(t['amount']))
        t_date = parse(t['date']).date()
        t_name = t['name']
        t_transaction_id = t['investment_transaction_id']
        s = self.transaction_securities[t_sec_id]
        s_ticker = s['ticker_symbol']
        s_name = s['name']
        t_quantity = amount.Amount(D(str(t['quantity'])), s_ticker)
        t_price = D(str(t['price']))
        postings = []

        postings.append(data.Posting(":".join((self.account_name, self.cash_acount_name)), -amount.Amount(t_amount, t_currency), None, None, None, None))
        cost = position.Cost(t_price, t_currency, t_date, None)
        postings.append(data.Posting(":".join((self.account_name, s_ticker)), t_quantity, cost, amount.Amount(t_price, t_currency), None, {'transaction_id': t_transaction_id}))
        txn = data.Transaction(meta, t_date, flags.FLAG_OKAY, s_name, t_name, data.EMPTY_SET, data.EMPTY_SET, postings)
        return txn

    def extract(self, filepath, existing):
        self.__build_commodity_table(existing)
        entries = []
        with open(filepath) as fp:
            j = json.load(fp)

            if 'investment_transactions' in j['response_types'] and 'investment_holdings' in j['response_types']:
                trans = j['investment_transactions']
                investment_holdings = j['investment_holdings']
                transactions = trans['investment_transactions']
                holdings = investment_holdings['holdings']

                self.transaction_securities = self.__build_security_table(trans['securities']) if 'securities' in trans else {}
                self.holding_securities = self.__build_security_table(investment_holdings['securities']) if 'securities' in investment_holdings else {}

                # Handle Transactions
                for index, t in enumerate(transactions):
                    t_type = t['type']
                    t_subtype = t['subtype']
                    meta = data.new_metadata(filepath, index)

                    trans_func = self.plaid_func_table.get((t_type, t_subtype))
                    assert trans_func is not None, f"Unhandled Investment transaction (type: { t_type }, subtype: { t_subtype } )"
                    tmp = trans_func(meta, t)
                    if tmp is not None:
                        entries.append(tmp)
                    #  TODO Add balance checks

                prices = self.__extract_prices(filepath, existing, holdings)
                entries.extend(prices)

            return entries

    def deduplicate(self, entries: data.Entries, existing: data.Entries) -> data.Entries:
        marked = []
        plaid_ids = set()

        # Create set of plaid_ids
        for entry in existing:
            if isinstance(entry, data.Transaction):
                for posting in entry.postings:
                    if 'transaction_id' in posting.meta:
                        plaid_ids.add(posting.meta['transaction_id'])

        for entry in entries:
            if isinstance(entry, data.Transaction):
                for posting in entry.postings:
                    if posting.meta is not None and 'transaction_id' in posting.meta:
                        if posting.meta['transaction_id'] in plaid_ids:
                            meta = entry.meta.copy()
                            meta[DUPLICATE] = True
                            entry = entry._replace(meta=meta)
            marked.append(entry)
        return marked


if __name__ == '__main__':
    importer = Importer('Assets:Current:Plaid401k',
                        'Bpa7oJZdJ7u8npdJvo1XCN31433Ga4fL7dk8N')
    main(importer)
