class SimpleMA(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data, period=20, 
                plotname="20 SMA")
        
cerebro.addstrategy(SimpleMA)