from ib_async import IB, Future, MarketOrder
import pandas as pd
from datetime import datetime, timezone

CSV_FILE = 'corn_price_data.csv'

def connect_ib_gateway():
    ib = IB()
    ib.connect('127.0.0.1', 4002, clientId=2)
    print("IB connected:", ib.isConnected())
    return ib

def get_latest_price(ib, contract):
    ticker = ib.reqTickers(contract)
    price = float(ticker[0].marketPrice())
    print(f"PRECIO DE HOY: {price}")
    return price

def place_order(ib, contract, action, quantity=1):
    order = MarketOrder(action, quantity)
    trade = ib.placeOrder(contract, order)
    while not trade.isDone():
        ib.sleep(1)
    print(f"Order {action} executed: {trade.orderStatus.status}")

def close_previous_position(ib, contract, prev_flag):
    if prev_flag == 1:
        print("Closing previous LONG position")
        place_order(ib, contract, 'SELL')
    elif prev_flag == -1:
        print("Closing previous SHORT position")
        place_order(ib, contract, 'BUY')

def MZC_contract(ib):
    contract = Future(symbol='MZC', lastTradeDateOrContractMonth='202509', exchange='CBOT', currency='USD')
    ib.qualifyContracts(contract)
    return contract

def cancel_open_orders(ib):

    return

def market_open():
    df = pd.read_csv("conr_price_data.csv", parse_dates=['date'])
    today_str = datetime.now(timezone.utc).date().strftime('%Y-%m-%d')
    today_row = df.loc[df['date'] == today_str]
    return today_row.iloc[0]['weekend_or_holiday'] == 0

def conseguir_precio_hoy():
    today = datetime.now(timezone.utc).date().strftime('%Y-%m-%d')
    df = pd.read_csv(CSV_FILE, parse_dates=['date'])
    today_row = df[df['date'] == today]

    if today_row.empty:
        print(f"❌ No entry for {today} in the CSV.")
        return

    ###
    ib = connect_ib_gateway() # ME CONECTO CON IBKR TODOS LOS DIAS
    ###
    
    if today_row.iloc[0]['weekend_or_holiday'] == 0:
        # Weekday — get price from IBKR
        contract = MZC_contract()
        price = get_latest_price(ib, contract)
        if price is None:
            print("❌ Could not retrieve price.")
            return
        df.loc[df['date'] == today, 'price'] = price
        print(f"✅ Saved live price {price} for {today}.")
    else:
        # Weekend or holiday — fill with previous valid price
        previous_rows = df[df['date'] < pd.to_datetime(today)]
        last_valid = previous_rows[previous_rows['weekend_or_holiday'] == 0].tail(1)

        if last_valid.empty or pd.isna(last_valid.iloc[0]['price']):
            print("⚠️ No previous valid price to fill.")
            return

        price = last_valid.iloc[0]['price']
        df.loc[df['date'] == today, 'price'] = price
        print(f"📌 {today} is a holiday. Filled with previous price: {price}")

    # Save updated CSV
    df.to_csv(CSV_FILE, index=False)
    return ib

def ib_disconnect(ib):
    # Me desconeccto todos los días.
    ib.disconnect()
    return "IB disconnected -> this should be false: ", ib.isConnected()

def bot(ib):
    df = pd.read_csv(CSV_FILE, parse_dates=['date'])

    today_str = datetime.now(timezone.utc).date().strftime('%Y-%m-%d')
    today_row = df.loc[df['date'] == today_str]

    if today_row.empty:
        raise Exception(f"Today's date {today_str} not found in CSV.")

    today_idx = today_row.index[0]
    flag_today = df.loc[today_idx, 'flag']
    prev_flag = df.loc[today_idx - 1, 'flag']

    #ib = connect_ib_gateway() ya me conecté antes!

    cancel_open_orders(ib) # cancelo cualquier orden no concretada de ayer. 

    contract = MZC_contract(ib)
    print(f"Contract is: {contract}")

    get_latest_price(ib, contract)

    if flag_today != prev_flag and prev_flag != 0:
        close_previous_position(ib, contract, prev_flag)

    if flag_today == 1 and prev_flag != 1:
        print("Placing new LONG order")
        #place_order(ib, contract, 'BUY')
    elif flag_today == -1 and prev_flag != -1:
        print("Placing new SHORT order")
        #place_order(ib, contract, 'SELL')
    else:
        print("flag 0: No trade executed today (if a position was opened yesterday, it was closed today)")

    # ib.disconnect() # me desconecto todos los dias si o si. 
    # print("IB disconnected -> this should be false:", ib.isConnected())

if __name__ == '__main__':
    bot()