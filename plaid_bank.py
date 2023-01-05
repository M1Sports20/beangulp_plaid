__copyright__ = "Copyright (C) 2022 Michael Spradling"
__license__ = "GNU GPLv2"

import json
import beangulp
from dateutil.parser import parse
from datetime import date, timedelta

from beancount.core import data
from beancount.core import amount
from beancount.core import flags
from beangulp.testing import main
from beancount.core.number import D


class Importer(beangulp.Importer):
    def __init__(self, account_name, account_id):
        self.account_name = account_name
        self.account_id = account_id

    def identify(self, filepath):
        with open(filepath) as fp:
            try:
                j = json.load(fp)
            except:
                return False

            if 'transactions' in j['response_types']:
                if "accounts" in j['transactions']:
                    if j['transactions']['accounts'][0]['account_id'] == self.account_id:
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

            # Get Balance
            balance = str(j['transactions']['accounts'][0]['balances']['current'])
            account_type = j['transactions']['accounts'][0]['type']
            currency = j['transactions']['accounts'][0]['balances']['iso_currency_code']
            last_date = date.min

            # Transactions
            for index, t in enumerate(j['transactions']['transactions']):
                # Ignore pending transactions
                if t['pending'] is True:
                    continue

                t_date = parse(t['date']).date()
                if t_date > last_date:
                    last_date = t_date
                merchant = t['merchant_name']
                description = t['name']
                amt = str(t['amount'])
                trans_id = t['transaction_id']
                meta = data.new_metadata(filepath, index)
                units = amount.Amount(D(amt), currency)
                pst_account = ':'.join(t['category']).replace(
                    "'", "").replace(',', '').replace(' ', '-')

                # GET Transaction to post
                leg1 = data.Posting(self.account_name, -units, None, None,
                                    None, {'transaction_id': trans_id})
                leg2 = data.Posting("Expenses:" + pst_account, units, None, None,
                                    None, None)
                txn = data.Transaction(meta, t_date, flags.FLAG_OKAY,
                                       merchant, description, data.EMPTY_SET,
                                       data.EMPTY_SET, [leg1, leg2])
                entries.append(txn)

            # Insert a final balance check
            if len(entries) != 0:
                meta = data.new_metadata(filepath, 0)
                amt = amount.Amount(D(balance), currency)
                if account_type == "credit" or account_type == "loan":
                    amt = -amt
                entries.append(data.Balance(meta, last_date + timedelta(days=1),
                                 self.account_name, amt, None, None))

        return entries


if __name__ == '__main__':
    importer = Importer('Assets:Current:PlaidSampleBank',
                        'lba8R6D568uraJgQw6RZfVjRBjjxzBurLjM89')
    main(importer)
