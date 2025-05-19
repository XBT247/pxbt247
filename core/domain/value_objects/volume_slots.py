from dataclasses import dataclass
from typing import List

@dataclass
class VolumeSlot:
    min_value: float
    max_value: float
    weight: float

@dataclass
class VolumeSlots:
    slots: List[VolumeSlot]
    
    def get_weight(self, volume: float) -> float:
        for slot in self.slots:
            if slot.min_value <= volume <= slot.max_value:
                return slot.weight
        return 0.0
    
    def from_json(self,volume_slots_json) -> 'VolumeSlots':
        """Convert JSON to VolumeSlots object"""
        slots = []
        for slot in volume_slots_json:
            slots.append(VolumeSlot(
                min_value=slot["min_value"],
                max_value=slot["max_value"],
                weight=slot["weight"]
            ))
        return VolumeSlots(slots=slots)