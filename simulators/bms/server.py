"""BMS HVAC simulator entrypoint — Modbus TCP slaves for every HVAC asset."""

import asyncio

from common.modbus_server import run

if __name__ == "__main__":
    asyncio.run(run("bms"))
