#!/usr/bin/env python

import sys
import argparse
import json
import http.server
import socketserver

import plaid
from plaid.api import plaid_api

from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.country_code import CountryCode
from plaid.model.products import Products


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--account', dest='account', required=True,
                        action='store', help='List of account names to download, defaults to all')
    parser.add_argument('-c', '--config', dest='config_file', default='settings.json',
                        help='Config file location, default (settings.json)')
    parser.add_argument('-l', '--list', action='store_true', help="List accounts")
    parser.add_argument('-o', '--output', default="plaid_auth.html", help="Plaid Auth HTML file to create")
    parser.add_argument('-u', '--update', action='store_true', help="Update Link only")
    parser.add_argument('-s', '--serve', action='store_true', help="Start Http server for plaid connection")
    args = parser.parse_args()

    # Load settings config file
    with open(args.config_file, "r") as config_fd:
        args.config = json.load(config_fd)

    if args.list is True:
        print("\n".join(args.config['accounts'].keys()))
        sys.exit(0)

    # If accounts are not specified, add all of them
    if args.account not in args.config['accounts'].keys():
        parser.error(f"Unknown Account: { args.account }")

    return args


def create_login_page(filename, link_token):
    with open(filename, 'w') as output:
        print("""
 <html>
    <body>
      <button id="linkButton">Open Link - Institution Select</button>
      <p id="results"></p>
      <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
      <script>
        var linkHandler = Plaid.create({
          token: '""" + link_token + """',
          onLoad: function () {
            // The Link module finished loading.
          },
          onSuccess: function (public_token, metadata) {
            // Send the public_token to your app server here.
            // The metadata object contains info about the institution the
            // user selected and the account ID, if selectAccount is enabled.
            console.log(
              "public_token: " +
                public_token +
                ", metadata: " +
                JSON.stringify(metadata)
            );
            document.getElementById("results").innerHTML =
              "public_token: " + public_token + "<br>metadata: " + metadata;
          },
          onExit: function (err, metadata) {
            // The user exited the Link flow.
            if (err != null) {
              // The user encountered a Plaid API error prior to exiting.
            }
            // metadata contains information about the institution
            // that the user selected and the most recent API request IDs.
            // Storing this information can be helpful for support.
          },
        });
        // Trigger the standard institution select view
        document.getElementById("linkButton").onclick = function () {
          linkHandler.open();
        };
      </script>
    </body>
 </html>""", file=output)


# Create a link_token for the given user
def create_link(client, access_token, update_link):
    if update_link:
        request = LinkTokenCreateRequest(
                client_name="BeanCount",
                country_codes=[CountryCode('US')],
                language='en',
                access_token=access_token,
                user=LinkTokenCreateRequestUser("BeanCountUser")
            )
        return client.link_token_create(request)['link_token']
    else:
        request = LinkTokenCreateRequest(
                client_name="BeanCount",
                country_codes=[CountryCode('US')],
                language='en',
                user=LinkTokenCreateRequestUser("BeanCountUser"),
                products=[Products('transactions'), Products('investments')],
            )
        return client.link_token_create(request)['link_token']


def main():
    args = parse_args()

    plaid_config = plaid.Configuration(
        host=args.config['plaid_config']['host'],
        api_key=args.config['plaid_config']['api_key'],
    )
    api_client = plaid.ApiClient(plaid_config)
    client = plaid_api.PlaidApi(api_client)

    access_token = args.config['accounts'][args.account]['access_token']
    link_token = create_link(client, access_token, args.update)
    create_login_page(args.output, link_token)
    print(f"Authenticate with Plaid using local file { args.output }...")

    if args.serve:
        try:
            with socketserver.TCPServer(("0.0.0.0", 5858), http.server.SimpleHTTPRequestHandler) as httpd:
                print(f"Serving Plaid Auth at http://[hostname]:5858/{ args.output }")
                httpd.serve_forever()
        except:
            pass
    
    if not args.update:
        public_token = input("What is the public_token: ")
        request = ItemPublicTokenExchangeRequest(public_token=public_token)
        response = client.item_public_token_exchange(request)
        access_token = response['access_token']
        item_id = response['item_id']
        print("Save the access_token to your settings.json file")
        print(f'access_token: "{access_token}",')
        print(f'item_id: "{item_id}"')

if __name__ == '__main__':
    main()
