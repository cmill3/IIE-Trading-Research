from datetime import timedelta, datetime
import datetime as dt
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
import pandas as pd
import helpers.backtraderhelpers as backtest
import matplotlib

# class BuyAndHold(bt.Strategy):
#     def start(self):
#         self.val_start = self.broker.get_cash()  # keep the starting cash

#     def nextstart(self):
#         # Buy all the available cash
#         size = int(self.broker.get_cash() / self.data)
#         self.buy(size=size)

#     def stop(self):
#         # calculate the actual returns
#         self.roi = (self.broker.get_value() / self.val_start) - 1.0
#         print('ROI:  {:.2f}%'.format(100.0 * self.roi))




# class backtraderstrat(bt.Strategy):

#     def __init__(self):
#         self.dataclose = self.data[0].close
#         self.order = None
#         self.buyprice = None
#         self.buycomm = None

#         self.truehigh = btind.truehigh()
#         self.truelow = btind.truelow()
#         self.roc = btind.ROC(data)
#         self.pctchange = btind.PctChange(period = 30)

#     def log(self, txt, dt=None):
#         #Logging function for the strat
#         dt = datetime or self.datas[0].datetime.date(0)
#         print('%s, %s' % (dt.isoformat(), txt))

#     def next(self):
#         if self.order:
#             return
#         if not self.position: # check if you already have a position in the market
#             if start_date == bt:
#                 self.log('Buy Create, %.2f' % self.data.close[0])
#                 self.order = self.buy(size=10) # buy when closing price today crosses above MA.
#             if (self.data.close[0] < self.ma[0]) & (self.data.close[-1] > self.ma[-1]):
#                 self.log('Sell Create, %.2f' % self.data.close[0])
#                 self.order = self.sell(size=10)  # sell when closing price today below MA
#         else:
# 		# This means you are in a position, and hence you need to define exit strategy here.
#             if len(self) >= (self.bar_executed + 4):
#                 self.log('Open, %.2f' % self.data.open[0])
#                 self.log('Position Closed, %.2f' % self.data.close[0])
#                 self.order = self.close()
	


# class MAstrategy(bt.Strategy):
# 	# when initializing the instance, create a 100-day MA indicator using the closing price
# 	def __init__(self):
# 		self.ma = bt.indicators.SimpleMovingAverage(self.data.close, period=100)
# 		self.order = None
 
# 	def next(self):
# 		if self.order:
# 			return
# 		if not self.position: # check if you already have a position in the market
# 			if (self.data.close[0] > self.ma[0]) & (self.data.close[-1] < self.ma[-1]):
# 				self.log('Buy Create, %.2f' % self.data.close[0])
# 				self.order = self.buy(size=10) # buy when closing price today crosses above MA.
# 			if (self.data.close[0] < self.ma[0]) & (self.data.close[-1] > self.ma[-1]):
# 				self.log('Sell Create, %.2f' % self.data.close[0])
# 				self.order = self.sell(size=10)  # sell when closing price today below MA
# 		else:
# 		# This means you are in a position, and hence you need to define exit strategy here.
# 			if len(self) >= (self.bar_executed + 4):
# 				self.log('Position Closed, %.2f' % self.data.close[0])
# 				self.order = self.close()
 
# 	# outputting information
# 	def log(self, txt):
# 		dt=self.datas[0].datetime.date(0)
# 		print('%s, %s' % (dt.isoformat(), txt))
   
# 	def notify_order(self, order):
# 		if order.status == order.Completed:
# 			if order.isbuy():
# 				self.log(
# 				"Executed BUY (Price: %.2f, Value: %.2f, Commission %.2f)" %
# 				(order.executed.price, order.executed.value, order.executed.comm))
# 			else:
# 				self.log(
# 				"Executed SELL (Price: %.2f, Value: %.2f, Commission %.2f)" %
# 				(order.executed.price, order.executed.value, order.executed.comm))
# 			self.bar_executed = len(self)
# 		elif order.status in [order.Canceled, order.Margin, order.Rejected]:
# 			self.log("Order was canceled/margin/rejected")
# 		self.order = None


rawdata = backtest.s3_data()
print(rawdata[0].time)
