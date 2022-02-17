# Work in progress for wrapping e-trade API: https://apisb.etrade.com/docs/api/market/api-quote-v1.html
import os
import json
from rauth import OAuth1Service
from rauth.session import OAuth1Session
import undetected_chromedriver.v2 as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from datetime import datetime
from datetime import timedelta
import time
import secret
from threading import *
import msgpack
import math
import jxmlease


class Etrader():
    '''Defines etrade data object, connects to user etrade account, keeps connection alive'''
    def __init__(self,  production=False, use_cached_session=True, delay_time_sec=2):
        self.use_cached_session = use_cached_session
        self.cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache.bin')
        self.consumer_key = secret.CONSUMER_KEY_PROD if production else secret.CONSUMER_KEY_DEV
        self.consumer_secret = secret.CONSUMER_SECRET_PROD if production else secret.CONSUMER_SECRET_DEV
        self.web_user = secret.WEB_USER
        self.web_password = secret.WEB_PASSWORD
        self.delay_time_sec = delay_time_sec
        self.session_start_time = datetime.now()
        self.base_url_prod = r"https://api.etrade.com"
        self.base_url_dev = r"https://apisb.etrade.com"
        self.__base_url = self.base_url_prod if production else self.base_url_dev
        self.__renew_access_token_url = r"https://api.etrade.com/oauth/renew_access_token"
        self.__revoke_access_token_url = r"https://api.etrade.com/oauth/revoke_access_token"
        self.service = OAuth1Service(
                  name='etrade',
                  consumer_key=self.consumer_key,
                  consumer_secret=self.consumer_secret,
                  request_token_url= "%s/oauth/request_token" % self.__base_url,
                  access_token_url="%s/oauth/access_token" % self.__base_url,
                  authorize_url='https://us.etrade.com/e/t/etws/authorize?key={}&token={}',
                  base_url=self.__base_url)
        self.oauth_token = None
        self.oauth_token_secret = None
        self.verifier = None
        self.session = None
        self.__authorization()
        self.current_account = self.__CurrentAccount()
        self.account_list = self.get_list_of_accounts()
        self.current_account.set_by_index(0)
        self.__auto_renew_auth = Timer(60, self.auto_renew_token)  # in new thread, after 1 hour start checking token every hour
        self.__auto_renew_auth.start()

    def __enter__(self):
        '''Permit WITH instantiation'''
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        '''Cleanup when object is destroyed'''
        self.__auto_renew_auth.cancel()
        if not self.use_cached_session:
            self.revoke_accesss_token()
            if os.path.isfile(self.cache_file):
                os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache.bin'))
        return

    def close(self):
        self.__exit__(None, None, None)
        return

    def __authorization(self):
        '''Authorize user session form cache if enabled or create new session'''
        def __retrieve_connection_cache():
            '''If previous session was cached and not destroyed, try to reopen session'''
            if os.path.isfile(self.cache_file):
                with open(self.cache_file, "rb") as cache_data:
                    byte_data = cache_data.read()
                    try:
                        con_data = msgpack.unpackb(byte_data)
                        self.oauth_token = con_data['oauth_token']
                        self.oauth_token_secret = con_data['oauth_token_secret']
                        self.service.authorize_url = con_data['authorize_url']
                        self.verifier = con_data['verifier']
                        self.session = OAuth1Session(**json.loads(json.dumps(con_data['session'])))
                    except Exception as e:
                        return False
                return True
            return False

        def __test_connection():
            '''test cached session validity by attempting to renew token'''
            res = self.renew_accesss_token()
            if res.status_code == 200:
                print('Using cached session.')
                return
            print('Cached session has expired.')
            __new_authorization()
            return

        def __reset_service():
            self.service = OAuth1Service(
                name='etrade',
                consumer_key=self.consumer_key,
                consumer_secret=self.consumer_secret,
                request_token_url="%s/oauth/request_token" % self.__base_url,
                access_token_url="%s/oauth/access_token" % self.__base_url,
                authorize_url='https://us.etrade.com/e/t/etws/authorize?key={}&token={}',
                base_url=self.__base_url)
            self.oauth_token = None
            self.oauth_token_secret = None
            self.verifier = None
            self.session = None
            return

        def __new_authorization():
            '''New authorization'''
            print('Starting new session authorization...')
            __reset_service()
            try:
                self.oauth_token, self.oauth_token_secret = self.service.get_request_token(params={'oauth_callback': 'oob', 'format': 'json'})
                self.service.authorize_url = self.service.authorize_url.format(self.consumer_key, self.oauth_token)
                self.verifier = self.__get_verifier()
                self.session = self.service.get_auth_session(self.oauth_token, self.oauth_token_secret, params={'oauth_verifier': self.verifier})
                self.session.headers.update({"Content-Type": "application/json", "consumerKey": self.consumer_key})
            except Exception as e:
                raise Exception(e)
                return False
            return True

        def __set_connection_cache():
            '''Write session parameters to disk if caching is enabled'''
            if os.path.isfile(self.cache_file):
                os.remove(self.cache_file)
            with open(self.cache_file, 'wb') as outfile:
                con_param = {'oauth_token': self.oauth_token,
                           'oauth_token_secret': self.oauth_token_secret,
                           'authorize_url': self.service.authorize_url,
                           'verifier': self.verifier,
                           'session': {'consumer_key': self.session.consumer_key,
                                       'consumer_secret': self.session.consumer_secret,
                                       'access_token': self.session.access_token,
                                       'access_token_secret': self.session.access_token_secret}}
                outfile.write(msgpack.packb(con_param))
            return

        if not self.use_cached_session:  # If not caching sessions, always get new token and never write to disk
            __new_authorization()
            return
        elif __retrieve_connection_cache():  # If using cache, test current values by attempting to renew previous token
            __test_connection()
        else:  # If token renewal fails, session has expired, required to go through new authorization
            __new_authorization()
        __set_connection_cache()  # write connection parameters to disk and update class variables with renewed or new session
        return

    def check_token(self):
        age = datetime.now() - self.session_start_time
        if age >= timedelta(hours=4):
            try:
                res = self.renew_accesss_token()
                if not res.ok:
                    self.__authorization()
            except Exception as e: # catches ConnectionError where remote host forcibly closes connection
                # Force new authorization
                if os.path.isfile(self.cache_file):
                    os.remove(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache.bin'))
                self.__authorization()
        return

    def __get_verifier(self):
        '''Use selenium web driver to make user copy oAuth confirmation code'''

        # configure undetectable web driver
        user_agent = f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4515.159 Safari/537.36'
        options = uc.ChromeOptions()
        options.headless = False
        options.add_argument('--no-first-run --no-service-autorun --password-store=basic --lang=en-US')
        driver = uc.Chrome(options=options)
        driver.options.add_argument(user_agent)
        # override header to remove headless word
        driver.execute_script("Object.defineProperty(navigator, 'userAgent', {get: function() {return '" + user_agent + "';}});")
        # verify user agent
        # user_agent = driver.execute_script("return navigator.userAgent")
        # initialize key action chains
        web_action = ActionChains(driver)

        def __user_login(driver):
            '''Login to etrade web ui with web credentials from secrets.py'''

            def __type_credentials(cred):
                '''Etrade bot dtetection detects the use of shift key, so we send creds accordingly'''
                for k in cred:
                    if k in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+~':
                        web_action.key_down(Keys.LEFT_SHIFT).key_down(k.lower()).key_up(k.lower()).key_up(Keys.LEFT_SHIFT).perform()
                    else:
                        web_action.key_down(k).key_up(k).perform()
                return

            driver.get(self.service.authorize_url)
            time.sleep(self.delay_time_sec)
            __type_credentials(self.web_user)
            web_action.key_down(Keys.TAB).key_up(Keys.TAB).perform()
            __type_credentials(self.web_password)
            web_action.key_down(Keys.RETURN).key_up(Keys.RETURN).perform()
            time.sleep(self.delay_time_sec)
            return

        def __agree_to_tos(driver):
            '''After UI login, users must hit button to agree to T.O.S'''
            agree_tos_btn = driver.find_element(By.XPATH, "//input[@value='Accept']")
            agree_tos_btn.click()
            time.sleep(self.delay_time_sec)
            return

        def __get_verifier_code(driver):
            '''After UI login, and acceptance of T.O.S, verifier code must be collected for input in next response'''
            verifier_box = driver.find_element(By.XPATH, "/html/body/div[2]/div/div/input")
            verifier_code = verifier_box.get_attribute('value')
            return verifier_code

        with driver:
            __user_login(driver)
            __agree_to_tos(driver)
            verifier_code = __get_verifier_code(driver)
        driver.close()
        return verifier_code

    def renew_accesss_token(self):
        '''renew_access_token'''
        self._session_start_time = datetime.now()
        return self.session.get(self.__renew_access_token_url)

    def auto_renew_token(self):
        while True:
            self.check_token()
            time.sleep(3600)  # check token every hour (3600 seconds)

    def revoke_accesss_token(self):
        '''revoke_access_token'''
        self.session.get(self.__revoke_access_token_url)
        return

    def get_list_of_accounts(self):
        '''Get all accounts related to consumer key'''

        def __get_list():
            end_pt = r"v1/accounts/list"
            api_url = "%s/%s.%s" % (self.__base_url, end_pt, 'json')
            req = self.session.get(api_url)
            req.raise_for_status()
            __update_current_account_obj(req.json()['AccountListResponse']['Accounts']['Account'])
            return

        def __populate_holdings():
            for i in range(len(self.account_list)):
                account_value = self.get_account_balance(self.account_list[i]['accountId'])
                self.account_list[i]['cashAvailable'] = account_value['Computed']['cashAvailableForInvestment']
                self.account_list[i]['positions'] = self.get_account_positions(self.account_list[i]['accountId'])
                self.account_list[i]['totalAccountValue'] = account_value['Computed']['RealTimeValues']['totalAccountValue']
            __update_current_account_obj(self.account_list)
            return

        def __update_current_account_obj(account_lst):
            self.account_list = account_lst
            self.current_account.update_account_list(account_lst)
            return

        __get_list()
        __populate_holdings()
        return self.account_list

    def get_account_balance(self, account_id=None):
        '''Get all account balances'''
        if account_id is not None:
            self.current_account.set_by_id(account_id)
        account_id_key = self.current_account.id_key
        account_inst_type = self.current_account.institution_type
        account_type = self.current_account.type
        end_pt = "v1/accounts"
        api_url = "%s/%s/%s/balance.json" % (self.__base_url, end_pt, account_id_key)
        payload = {"realTimeNAV": True, "instType": account_inst_type, "accountType": account_type}
        req = self.session.get(api_url, params=payload)
        req.raise_for_status()
        return req.json()['BalanceResponse']

    def get_account_positions(self, account_id=None):
        '''Get account positions'''
        if account_id is not None:
            self.current_account.set_by_id(account_id)
        end_pt = "v1/accounts"
        api_url = "%s/%s/%s/portfolio.json" % (self.__base_url, end_pt, self.current_account.id_key)
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()['PortfolioResponse']['AccountPortfolio'][0]['Position']

    def get_account_transaction_history(self, account_id=None, ticker_symbol=None):
        '''Get Transaction History'''
        if account_id is not None:
            self.current_account.set_by_id(account_id)
        end_pt = "v1/accounts"
        api_url = "%s/%s/%s/transactions.json" % (self.__base_url, end_pt, self.current_account.id_key)
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()['TransactionListResponse']['Transaction']

    def get_transaction_details(self, transaction_id=None, account_id=None,):
        '''Get Transaction History'''
        if transaction_id is None:
            return []
        if account_id is not None:
            self.current_account.set_by_id(account_id)
        end_pt = "v1/accounts"
        api_url = "%s/%s/%s/transactions/%s.json" % (self.__base_url, end_pt, self.current_account.id_key, transaction_id)
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()['TransactionDetailsResponse']

    def get_existing_orders(self, account_id=None):
        '''Get existing orders in account'''
        if account_id is not None:
            self.current_account.set_by_id(account_id)
        end_pt = "v1/accounts"
        api_url = "%s/%s/%s/orders.json" % (self.__base_url, end_pt, self.current_account.id_key)
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()['OrdersResponse']['Order'] if req.content else []

    def get_quote(self, stock_ticker: list or tuple or str) -> dict:
        '''Get market quote for provided stock ticker'''
        if not isinstance(stock_ticker, list) and not isinstance(stock_ticker, tuple):
            stock_ticker = [stock_ticker]
        stock_ticker = ','.join(stock_ticker)
        end_pt = "v1/market/quote"
        api_url = "%s/%s/%s.json" % (self.__base_url, end_pt, stock_ticker)
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()['QuoteResponse']['QuoteData'] if req.content else []

    def look_up_product(self, search_str: str) -> dict:
        '''Performs a look up product'''
        # api_url = self.base_url + "lookup/%s" % search_str
        api_url = "%slookup/%s" % (self.__base_url, search_str + ".json")
        req = self.session.get(api_url)
        req.raise_for_status()
        return req.json()

    def list_orders(self, count=100):
        '''lists all orders of self.current_account up to count value'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders.json'
        req = self.session.get(api_url, params={'count': count}, timeout=30)
        req.raise_for_status()
        return req.json()['OrdersResponse'] if req.content else []

    def list_open_orders(self, count=100):
        '''lists all OPEN orders of self.current_account up to count value'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders.json'
        req = self.session.get(api_url, params={'count': count, 'status': 'OPEN'}, timeout=30)
        req.raise_for_status()
        return req.json()['OrdersResponse'] if req.content else []

    def list_executed_orders(self, count=100):
        '''lists all EXECUTED orders of self.current_account up to count value'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders.json'
        req = self.session.get(api_url, params={'count': 100, 'status': 'EXECUTED'}, timeout=30)
        req.raise_for_status()
        return req.json()['OrdersResponse'] if req.content else []

    def list_ticker_orders(self, ticker, count=100):
        '''lists all orders of TICKER in self.current_account up to count value'''
        # api_url = self.base_url + "lookup/%s" % search_str
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders.json'
        req = self.session.get(api_url, params={'count': count, 'symbol':ticker}, timeout=30)
        req.raise_for_status()
        return req.json()['OrdersResponse'] if req.content else []

    def preview_order(self, symbol, order_action, num_shares, price_type='MARKET', limit_price='', stop_price='', market_session='REGULAR', order_term='GOOD_UNTIL_CANCEL', all_or_none=False, preview_id=None, unique_id=None):
        '''Construct Order on ETRADE before executing'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders/preview.json'
        instrument = {'Product': {'securityType': 'EQ', 'symbol':symbol},
                     'orderAction': order_action,
                     'quantityType': 'QUANTITY',
                     'quantity': num_shares
                      }
        order = {'allOrNone': str(all_or_none).lower(),
                 'priceType': price_type,
                 'orderTerm': order_term,
                 'marketSession': market_session,
                 'stopPrice': stop_price,
                 'limitPrice': limit_price,
                 'Instrument': instrument
                 }
        payload = {'PreviewOrderRequest': {'orderType': 'EQ',
                                           'clientOrderId': unique_id if unique_id is not None else datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
                                           'Order': order}}
        payload = jxmlease.emit_xml(payload)

        headers = {"Content-Type": "application/xml", "consumerKey": self.consumer_key}
        req = self.session.post(api_url, header_auth=True, headers=headers, data=payload)
        req.raise_for_status()
        if 'error' in req.text.lower():
            raise ValueError(json.loads(req.text))
        return req.json()['PreviewOrderResponse'] if req.content else []

    def __execute_previewd_order(self, order_obj, unique_id=None):
        '''Execute previously previewed order'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders/place.json'

        payload = {'PlaceOrderRequest': order_obj}
        payload['PlaceOrderRequest']['Order'] = payload['PlaceOrderRequest']['Order'][0]
        payload['PlaceOrderRequest']['clientOrderId'] = unique_id if unique_id is not None else datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        payload = jxmlease.emit_xml(payload)

        headers = {"Content-Type": "application/xml", "consumerKey": self.consumer_key}
        req = self.session.post(api_url, header_auth=True, headers=headers, data=payload)
        req.raise_for_status()
        req_json = req.json()
        req = req_json['PlaceOrderResponse']['Order'][0]
        req['orderId'] = req_json['PlaceOrderResponse']['OrderIds'][0]['orderId']
        return req

    def __update_account_info(self):
        self.current_account.update_account_list(self.get_list_of_accounts())
        self.current_account.set_by_id_key(self.current_account.id_key)
        return

    def __available_shares_by_symbol(self, symbol):
        open_orders = self.list_open_orders()['Order'] if self.list_open_orders() else []
        current_holdings = [t['quantity'] for t in self.current_account.positions if t['symbolDescription'] == symbol]
        current_holdings = sum(current_holdings) if len(current_holdings) > 0 else 0
        if not open_orders:
            return current_holdings
        allocated_shares = [x['OrderDetail'][0]['Instrument'][0]['orderedQuantity'] for x in open_orders if x['OrderDetail'][0]['Instrument'][0]['Product']['symbol'] == symbol]
        allocated_shares = sum(allocated_shares) if len(allocated_shares) > 0 else 0
        return current_holdings-allocated_shares

    def place_market_buy_order(self, symbol, dollar_amount):
        '''Place Market BUY order for ticker with maximum number of shares per given dollar amount'''
        current_price = self.get_quote(symbol)[0]['All']['ask']
        funds = self.current_account.cash_available
        num_shares = self.__calc_number_of_shares(current_price, min(dollar_amount, funds))

        if num_shares <= 0:
            print(f'Insufficient funds! Security cost: {current_price} > Allocated Funds: {dollar_amount} OR Cash Available: {funds}')
            return []

        response = self.__execute_previewd_order(self.preview_order(symbol, 'BUY', num_shares, price_type='MARKET'))
        self.__update_account_info()
        return response

    def place_market_sell_order(self, symbol, num_shares):
        '''Place Market SELL order for ticker with number of shares per given'''
        num_shares = min(num_shares, self.__available_shares_by_symbol(symbol))

        if num_shares <= 0:
            print(f'No existing holdings of: {symbol}')
            return []

        req = self.__execute_previewd_order(self.preview_order(symbol, 'SELL', num_shares, price_type='MARKET'))
        self.__update_account_info()
        return req

    def place_limit_buy_order(self, symbol, num_shares, price_limit):
        '''Place LIMIT BUY order for ticker with given number of shares'''
        funds = self.current_account.cash_available
        expected_num_shares = self.__calc_number_of_shares(price_limit, funds)
        num_shares = min(num_shares, expected_num_shares)

        if num_shares <= 0:
            print(f'Insufficient funds for purchase of single product: {symbol} at: ${price_limit}.  Current cash: ${funds}')
            return []

        req = self.__execute_previewd_order(self.preview_order(symbol, 'BUY', num_shares, limit_price=price_limit, price_type='LIMIT'))
        self.__update_account_info()
        return req

    def place_limit_sell_order(self, symbol, num_shares, price_limit):
        '''Place LIMIT SELL order for ticker with given number of shares'''
        num_shares = min(num_shares, self.__available_shares_by_symbol(symbol))

        if num_shares <= 0:
            print(f'No existing holdings of: {symbol}')
            return []

        req = self.__execute_previewd_order(self.preview_order(symbol, 'SELL', num_shares, limit_price=price_limit, price_type='LIMIT'))
        self.__update_account_info()
        return req

    def cancel_order(self, order_number):
        '''Cancel Executed Order'''
        end_pt = "v1/accounts"
        api_url = f'{self.__base_url}/{end_pt}/{self.current_account.id_key}/orders/cancel.json'
        payload = jxmlease.emit_xml({"CancelOrderRequest": {"orderId": order_number}})
        headers = {"Content-Type": "application/xml", "consumerKey": self.consumer_key}
        req = self.session.put(api_url, header_auth=True, headers=headers, data=payload)
        req.raise_for_status()
        self.__update_account_info()
        return req.json()['CancelOrderResponse']

    class __CurrentAccount(object):
        def __init__(self, account_list=None):
            self.id = None
            self.id_key = None
            self.description = None
            self.mode = None
            self.name = None
            self.status = None
            self.type = None
            self.institution_type = None
            self.closed_date = None
            self.cash_available = None
            self.positions = None
            self.total_account_value = None
            self.__account_list = account_list

        def __call__(self):
            return self.get()

        def get(self):
            return {'id': self.id, 'id_key': self.id_key, 'description': self.description, 'mode': self.mode,
                    'name': self.name, 'status': self.status, 'type': self.type,
                    'institution_type': self.institution_type, 'closed_date': self.closed_date,
                    'cash_available': self.cash_available, 'total_account_value': self.total_account_value,
                    'positions': self.positions}

        def set(self, account_dict):
            self.id = account_dict['accountId'] if 'accountId' in account_dict else None
            self.id_key = account_dict['accountIdKey'] if 'accountIdKey' in account_dict else None
            self.description = account_dict['accountDesc'] if 'accountDesc' in account_dict else None
            self.mode = account_dict['accountMode'] if 'accountMode' in account_dict else None
            self.name = account_dict['accountName'] if 'accountName' in account_dict else None
            self.status = account_dict['accountStatus'] if 'accountStatus' in account_dict else None
            self.type = account_dict['accountType'] if 'accountType' in account_dict else None
            self.institution_type = account_dict['institutionType'] if 'institutionType' in account_dict else None
            self.closed_date = account_dict['closedDate'] if 'closedDate' in account_dict else None
            if 'cashAvailable' in account_dict:
                self.cash_available = account_dict['cashAvailable']
            if 'positions' in account_dict:
                self.positions = account_dict['positions']
            if 'totalAccountValue' in account_dict:
                self.total_account_value = account_dict['totalAccountValue']
            return

        def set_by_id(self, id):
            account_dict = None
            idx = 0
            for i in range(len(self.__account_list)):
                if self.__account_list[i]['accountId'] == id:
                    account_dict = self.__account_list[i]
                    idx = i
                    break
            if account_dict is None:
                raise ValueError(f'Invalid account ID: {id}')
            else:
                self.set(self.__account_list[idx])
            return

        def set_by_id_key(self, id_key):
            account_dict = None
            idx = 0
            for i in range(len(self.__account_list)):
                if self.__account_list[i]['accountIdKey'] == id_key:
                    account_dict = self.__account_list[i]
                    idx = i
                    break
            if account_dict is None:
                raise ValueError(f'Invalid account ID Key: {id_key}')
            else:
                self.set(self.__account_list[idx])
            return

        def set_by_index(self, index):
            try:
                self.set(self.__account_list[index])
            except Exception as e:
                raise ValueError(
                    f'Account index is out of range. Expecting value between 0 and {len(self.__account_list)}')
            return

        def update_account_list(self, account_list):
            self.__account_list = account_list
            return

    @staticmethod
    def __calc_number_of_shares(price, funds):
        return math.floor(funds/price)

