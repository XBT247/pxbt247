# application/controllers/trade_controller.py
from core.domain.trade import Trade
from core.use_cases.process_trades import ProcessTradeUseCase

class TradeMessageController:
    def __init__(self, use_case: ProcessTradeUseCase):
        self.use_case = use_case

    async def handle(self, message: dict):
        trade = Trade(
            symbol=message['key'],
            price=message['value']['price'],
            quantity=message['value']['quantity'],
            timestamp=message['value']['timestamp'],
            is_buyer_maker=message['value']['is_buyer_maker']
        )
        await self.use_case.execute(trade)