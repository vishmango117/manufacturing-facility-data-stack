"""EMS gateway — per-machine Modbus master -> Avro -> Kafka publisher."""

import asyncio

from common.publisher import run

if __name__ == "__main__":
    asyncio.run(run("ems"))
