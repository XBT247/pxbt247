import unittest
from unittest.mock import AsyncMock, MagicMock
from application.services.volume_points_calculator import VolumePointsCalculator, VolumeSlot

class TestVolumePointsCalculator(unittest.IsolatedAsyncioTestCase):  # Use IsolatedAsyncioTestCase
    def setUp(self):
        # Mock repositories
        self.mock_trading_pairs_repo = MagicMock()
        self.mock_trades_repo = MagicMock()
        
        # Create an instance of VolumePointsCalculator
        self.calculator = VolumePointsCalculator(
            trading_pairs_repo=self.mock_trading_pairs_repo,
            trades_repo=self.mock_trades_repo
        )
    
    def test_create_dynamic_slots(self):
        # Test data
        values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        slot_count = 5
        min_weight = 0.5
        max_weight = 11.0
        min_percentile = 0
        max_percentile = 100
        
        # Call the method
        slots = self.calculator._create_dynamic_slots(
            values=values,
            slot_count=slot_count,
            min_weight=min_weight,
            max_weight=max_weight,
            min_percentile=min_percentile,
            max_percentile=max_percentile
        )
        
        # Assertions
        self.assertEqual(len(slots), 4)  # Update expected value if merging logic is correct
        self.assertAlmostEqual(slots[0].min_value, 10.0, places=2)
        self.assertAlmostEqual(slots[-1].max_value, 100.0, places=2)
        self.assertTrue(all(isinstance(slot, VolumeSlot) for slot in slots))
    
    async def test_save_slots_to_db(self):
        # Mock the repository's update_slots_data method
        self.calculator.repo.update_slots_data = AsyncMock()
        
        # Mock data
        symbol = "BTCUSD"
        self.calculator.volume_slots[symbol] = [
            VolumeSlot(min_value=10.0, max_value=20.0, weight=1.0, count=5, percentile_min=0.0, percentile_max=10.0),
            VolumeSlot(min_value=20.0, max_value=30.0, weight=2.0, count=10, percentile_min=10.0, percentile_max=20.0)
        ]
        self.calculator.trades_slots[symbol] = [
            VolumeSlot(min_value=5.0, max_value=15.0, weight=1.5, count=8, percentile_min=0.0, percentile_max=10.0)
        ]
        self.calculator.median_vpt[symbol] = 15.0
        self.calculator.vpt_std_dev[symbol] = 5.0
        
        # Call the method
        await self.calculator._save_slots_to_db(symbol)
        
        # Assertions
        self.calculator.repo.update_slots_data.assert_awaited_once()
        args = self.calculator.repo.update_slots_data.call_args[1]
        self.assertIn("volume_slots", args)
        self.assertIn("trade_slots", args)
        self.assertIn("stats", args)

if __name__ == "__main__":
    unittest.main()