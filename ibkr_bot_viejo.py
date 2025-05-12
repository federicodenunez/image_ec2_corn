import asyncio
from ib_async import IB, Future, MarketOrder, util
import numpy as np

async def connect_ib_gateway():
    # util.startLoop() # esto lo añadi del tutorial
    ib = IB()
    await ib.connect('127.0.0.1', 4002, client_id=1)
    return ib

async def get_latest_price(ib, contract):
    ticker = await ib.reqTickers(contract) # ib.reqMktData #ib.sleep(2) para q se cargue pero creo q el await lo soluciona.
    return float(ticker[0].marketPrice()) # @Gerar combiene usar otro?

async def place_order(ib, contract, action, quantity=1): # quantity=1 -> significa 500
    order = MarketOrder(action, quantity) # MarketOrder solo se va a ejecutar si estamos en trading hours sino no y nos va a quedar del día anterior. 
    # Podríamos limpiar ordenes viejas no ejecutadas por las dudas cada vez que conectamos? 
    trade = await ib.placeOrder(contract, order)
    while not trade.isDone():
        await asyncio.sleep(1)
        # print(trade.orderStatus) # por si queremos ver el estado
        #ib.waitOnUpdate()
    print(f"Order {action} executed: {trade.orderStatus.status}")

async def cancelar_ordenes_viejas(ib):
    list_of_open_orders = ib.openOrders()
    if len(list_of_open_orders) > 0:
        for order in list_of_open_orders:
            ib.cancelOrder(order)
            print(f"Cancelada la orden {order}")

async def bot(flag):
    # Conectamos con IB_Gateway
    ib = await connect_ib_gateway()

    # Cancelo ordenes abiertas (no debería haber ninguna lo hago por las dudas para que no se ejecuten)
    cancelar_ordenes_viejas(ib)

    # hardcodeamos el contrato
    contract = Future(symbol='MZC', lastTradeDateOrContractMonth='202509', exchange='CBOT', currency='USD') # MZCU25 o no hace falta?
    # debería ser un solo contract
    #print(contract) # deberia ser 1 solo
    await ib.qualifyContractsAsync(contract) # Esto devuelve el contrato preciso que tenemos con los parametros de arriba. Deberia ser uno solo por lastTradeDate
    # sacar el async?

    #price = await get_latest_price(ib, contract)
    #print(f"Current MZCU25 Price: {price}")

    if flag == 1:
        print("Placing a LONG order")
        #await place_order(ib, contract, 'BUY')
    elif flag == -1:
        print("Placing a SHORT order")
        #await place_order(ib, contract, 'SELL')
    else:
        print("No trade today (flag=0)")

    await ib.disconnect()

if __name__ == '__main__':
    flag1 = np.random.choice([-1, 0, 1]) # Modelo Franco
    flag2 = np.random.choice([-1, 0, 1]) # Modelo Gerar
    asyncio.run(bot(flag1))
    asyncio.run(bot(flag2))

### Para cancelar una orden:
"""
for order in ib.openOrders():
    ib.cancelOrder(order)
"""