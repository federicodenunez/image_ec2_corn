import asyncio
from ib_async import IB, Future, MarketOrder
import pandas as pd
from datetime import datetime, timezone

CSV_FILE = 'corn_price_data.csv'

async def connect_ib_gateway():
    ib = IB()
    await ib.connect('127.0.0.1', 4002, client_id=1)
    print("IB CONNECTED SUCCESFULLY")
    return ib

async def get_latest_price(ib, contract):
    ticker = await ib.reqTickers(contract)
    print(f"PRECIO DE HOOOOY: {float(ticker[0].marketPrice())}")
    return float(ticker[0].marketPrice())

async def place_order(ib, contract, action, quantity=1):
    order = MarketOrder(action, quantity)
    trade = await ib.placeOrder(contract, order)
    while not trade.isDone():
        await asyncio.sleep(1)
    print(f"Order {action} executed: {trade.orderStatus.status}")

async def close_previous_position(ib, contract, prev_flag):
    if prev_flag == 1:
        print("Closing previous LONG position")
        await place_order(ib, contract, 'SELL')
    elif prev_flag == -1:
        print("Closing previous SHORT position")
        await place_order(ib, contract, 'BUY')

async def MZC_contract(ib):
    contract = Future(symbol='MZC', lastTradeDateOrContractMonth='202509', exchange='CBOT', currency='USD')
    await ib.qualifyContractsAsync(contract)
    return contract

async def bot():
    df = pd.read_csv(CSV_FILE, parse_dates=['date'])

    today_str = datetime.now(timezone.utc).date().strftime('%Y-%m-%d')
    today_row = df.loc[df['date'] == today_str]

    if today_row.empty:
        raise Exception(f"Today's date {today_str} not found in CSV.")

    today_idx = today_row.index[0]
    flag_today = 1 #df.at[today_idx, 'flag']

    prev_flag = 0 #df.at[today_idx - 1, 'flag'] if today_idx > 0 else 0

    ib = await connect_ib_gateway()

    contract = Future(symbol='MZC', lastTradeDateOrContractMonth='202509', exchange='CBOT', currency='USD')
    await ib.qualifyContractsAsync(contract)
    print(f"Contract is: {contract}")

    # Close previous position if flag changed
    if flag_today != prev_flag and prev_flag != 0:
        await close_previous_position(ib, contract, prev_flag)

    # Open new position if necessary
    if flag_today == 1 and prev_flag != 1:
        print("Placing new LONG order")
        #await place_order(ib, contract, 'BUY')
    elif flag_today == -1 and prev_flag != -1:
        print("Placing new SHORT order")
        #await place_order(ib, contract, 'SELL')
    else:
        print("flag 0: No trade executed today (if a position was opened yesterday, it was closed today)")

    await ib.disconnect()

if __name__ == '__main__':
    asyncio.run(bot())