"""Reusable Modbus TCP slave server for the simulators.

Hosts every device of a given source (bms|ems) as its own Modbus slave *unit id*
on a single TCP endpoint (the standard way a field gateway fronts many devices).
A background task refreshes each device's holding registers on a fixed cadence so
the gateway's polls return live, evolving values.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartAsyncTcpServer

from .generators import EnergyIntegrator, bms_metric_value, ems_power
from .modbus_codec import encode_metrics
from .registry import Device, devices_for

log = logging.getLogger("modbus_server")

# Holding-register block sized for the widest device (registers = metrics * 2).
_BLOCK_REGISTERS = 64
_FAULT_PROBABILITY = float(os.environ.get("FAULT_PROBABILITY", "0.01"))


def _build_context(devices: list[Device]) -> ModbusServerContext:
    slaves = {}
    for d in devices:
        block = ModbusSequentialDataBlock(0, [0] * _BLOCK_REGISTERS)
        slaves[d.unit_id] = ModbusSlaveContext(hr=block, zero_mode=True)
    return ModbusServerContext(slaves=slaves, single=False)


def _compute_values(d: Device, now: float, integ: EnergyIntegrator) -> list[float]:
    if d.source == "ems":
        # totalPower then totalEnergy (integrated from power).
        power_spec = d.metrics[0]
        power = ems_power(d.equipment_type, power_spec.lo, power_spec.hi,
                          power_spec.nominal, now)
        energy = integ.add(d.device_id, power, now)
        return [power, energy]
    import random
    fault = random.random() < _FAULT_PROBABILITY
    return [bms_metric_value(m, now, fault=fault) for m in d.metrics]


async def _refresh_loop(context: ModbusServerContext, devices: list[Device],
                        interval: float) -> None:
    integ = EnergyIntegrator()
    while True:
        now = time.time()
        for d in devices:
            values = _compute_values(d, now, integ)
            registers = encode_metrics(values)
            # function code 3 == holding registers
            context[d.unit_id].setValues(3, 0, registers)
        await asyncio.sleep(interval)


async def run(source: str) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    host = os.environ.get("MODBUS_HOST", "0.0.0.0")
    port = int(os.environ.get("MODBUS_PORT", "5020"))
    interval = float(os.environ.get("REFRESH_INTERVAL", "5"))

    devices = devices_for(source)
    context = _build_context(devices)
    log.info("Starting %s Modbus slave on %s:%s with %d device units",
             source, host, port, len(devices))

    asyncio.create_task(_refresh_loop(context, devices, interval))
    await StartAsyncTcpServer(context=context, address=(host, port))
