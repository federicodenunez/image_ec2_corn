from ib_async import IB, util

async def connect_ib_gateway():
    ib = IB()
    await ib.connect('127.0.0.1', 4002, clientId=1)
    print("IB connected:", ib.isConnected())
    await ib.disconnect()
    print("IB disconnected.")

if __name__ == '__main__':
    util.run(connect_ib_gateway())