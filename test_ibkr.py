from ib_async import IB, util
import asyncio

def kill_existing_event_loop():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            print("Killing existing running event loop...")
            # Cancel all running tasks
            tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in tasks:
                task.cancel()
            # Wait for all tasks to be cancelled properly
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # Close the loop
            loop.stop()
            loop.close()
            print("Existing event loop successfully closed.")
    except Exception as e:
        print(f"No running loop or could not close it cleanly: {e}")
    finally:
        # Ensure a fresh clean loop will be created when needed
        asyncio.set_event_loop(asyncio.new_event_loop())

async def connect_ib_gateway():
    ib = IB()
    await ib.connect('127.0.0.1', 4002, clientId=1)
    print("IB connected:", ib.isConnected())
    await ib.disconnect()
    print("IB disconnected.")

if __name__ == '__main__':
    print("Eliminando loops")
    kill_existing_event_loop()
    print("LOOPS ELIMINADOS")
    asyncio.run(connect_ib_gateway())