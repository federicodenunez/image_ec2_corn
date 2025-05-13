from ib_async import IB, util

async def connect_ib_gateway():
    ib = IB()
    await ib.connect('127.0.0.1', 4002, 1)
    print(ib.isConnected())
    print("IB connected successfully.")
    await ib.disconnect()
    print("IB Disconnected succesfully.")
    print(ib.isConnected())

if __name__ == '__main__':
    util.run(connect_ib_gateway())