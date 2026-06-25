"""Modbus register codec.

Each metric is encoded as a 32-bit IEEE-754 float across two consecutive holding
registers (big-endian word order), the standard convention for analogue points on
Modbus field devices. Keeps simulator (slave) and gateway (master) in lock-step.
"""

from __future__ import annotations

import struct
from typing import List


def float_to_registers(value: float) -> List[int]:
    """Encode a float into two 16-bit registers (big-endian)."""
    raw = struct.pack(">f", float(value))
    hi, lo = struct.unpack(">HH", raw)
    return [hi, lo]


def registers_to_float(registers: List[int]) -> float:
    """Decode two 16-bit registers (big-endian) back into a float."""
    raw = struct.pack(">HH", registers[0] & 0xFFFF, registers[1] & 0xFFFF)
    return struct.unpack(">f", raw)[0]


def encode_metrics(values: List[float]) -> List[int]:
    """Flatten an ordered list of metric values into a register block."""
    block: List[int] = []
    for v in values:
        block.extend(float_to_registers(v))
    return block


def decode_metrics(registers: List[int], count: int) -> List[float]:
    """Decode `count` floats from a register block."""
    return [registers_to_float(registers[i * 2:i * 2 + 2]) for i in range(count)]
