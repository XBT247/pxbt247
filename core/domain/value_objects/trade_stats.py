from dataclasses import dataclass
from typing import List

@dataclass
class TradeSlots:
    min_value: float
    max_value: float
    weight: float

@dataclass
class TradeSlots:
    slots: List[TradeSlots]
    
    def get_weight(self, volume: float) -> float:
        for slot in self.slots:
            if slot.min_value <= volume <= slot.max_value:
                return slot.weight
        return 0.0
    
    def from_json(self,volume_slots_json) -> 'TradeSlots':
        """Convert JSON to TradeSlots object"""
        slots = []
        for slot in volume_slots_json:
            slots.append(TradeSlots(
                min_value=slot["min_value"],
                max_value=slot["max_value"],
                weight=slot["weight"]
            ))
        return TradeSlots(slots=slots)