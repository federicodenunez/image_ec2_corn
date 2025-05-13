from ib_async import IB, util
import asyncio


ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)
print("IB connected:", ib.isConnected())
ib.disconnect()
print("IB disconnected.")
