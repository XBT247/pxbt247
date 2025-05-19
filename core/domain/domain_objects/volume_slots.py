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
    
    def get_weight_for_volume(self, volume: float) -> float:
        for slot in self.slots:
            if slot.min_value <= volume <= slot.max_value:
                return slot.weight
        return 0.0