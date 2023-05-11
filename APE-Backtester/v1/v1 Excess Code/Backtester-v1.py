from datetime import timedelta, datetime
import datetime as dt
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
import pandas as pd
import helpers.backtraderhelpers as backtest
import matplotlib

def pull_data():
    rawdata = backtest.s3_data()
    start_date = datetime.strptime(rawdata['date'].values[1], '%Y-%m-%d %H:%M:%S')
    end_date = backtest.end_date(start_date, 4)
    symbol = rawdata['symbol'].values[1]
    mkt_price = rawdata['regularMarketPrice'].values[1]
    strategy = rawdata['title'].values[1]
    option_symbol, stratdirection, polygon_df = backtest.data_pull(symbol, start_date, end_date, mkt_price, strategy)
    return start_date, end_date, symbol, mkt_price, strategy, option_symbol, stratdirection, polygon_df

start_date, end_date, symbol, mkt_price, strategy, optionsymbol, stratdirection, polygon_df = pull_data()

print(polygon_df)
open_prices = polygon_df['o'].values
print(stratdirection)
data = bt.feeds.GenericCSVData(
    dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_RBLX_2023-01-02 14:00:00.csv',
    fromdate=start_date,
    todate=end_date,
    sessionstart=start_date,
    sessionend=end_date,
    dtformat=('%Y-%m-%d %H:%M:%S'),
    datetime=9,
    open = 3,
    high = 5,
    low = 6,
    close = 4,
    volume =1, 
    openinterest=-1,
    reverse=False)

class InAndOut(bt.Strategy):
    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = self.datas[0].datetime.date(0)
        dt_time = self.datas[0].datetime.time(0)
        dt_weekday = datetime.weekday(dt)


        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.datatest = self.datas[0]
        # self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.dataopens = open_prices
        self.openpurchaseprice = self.dataopens[0]
        self.order = None
        self.startdt = self.datas[0].datetime
        self.enddt = self.datas[-1].datetime
        self.dt = self.datas[0].datetime
        self.stratdirection = stratdirection
        if self.stratdirection == 'C':
            self.target_price = mkt_price * 1.05
        elif self.stratdirection == 'P':
            self.target_price = mkt_price - (0.05 * mkt_price)
        print(self.datatest)
        print(self.startdt)
        print(self.openpurchaseprice)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, Datetime: %s' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm,
                    bt.num2date(order.executed.dt)))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, Datetime: %s' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm,
                          bt.num2date(order.executed.dt)))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))
        
    def start(self):
        self.val_start = self.broker.get_cash()  # keep the starting cash
    
    def nextstart(self):
        if not self.position:
            self.buy(price = self.openpurchaseprice, size = 100)
            print(self.dataopens[0])
        else:
            return

    def next(self):
        if self.order:
            return
        if self.position:
            if self.dataopen >= self.target_price:
                self.sell(size = 100)
                # print(self.broker.getposition(data))
            else:
                return
        else:
            self.close(size = 100)
            # print(self.broker.getposition(data))
        # if not self.position:
        #     self.buy(price = self.dataopen[0], size = 100)
        #     print(self.dataopen[0])
        #     # print(self.broker.getposition(data))
        # elif self.position:
        #     if self.dataopen >= self.target_price:
        #         self.sell(size = 100)
        #         # print(self.broker.getposition(data))
        #     else:
        #         return
        # else:
        #     self.close(size = 100)
        #     # print(self.broker.getposition(data))

    
    # def next(self):
    #     self.close(size = 100)
    #     print(self.broker.getposition(data))

    def stop(self):
        print(self.broker.get_orders_open(True))
        print(self.broker.getposition(data))
        self.val_end = self.broker.get_cash()
        # print(self.broker.get_value())
        # print(self.val_start)
        # print(self.val_end)
        self.roi = (self.val_end - self.val_start)/100
        print('ROI:  {:.2f}%'.format(self.roi))

# class InAndOut(bt.Strategy):
#     def log(self, txt, dt=None):
#         ''' Logging function for this strategy'''
#         dt = self.datas[0].datetime.date(0)
#         dt_time = self.datas[0].datetime.time(0)
#         dt_weekday = datetime.weekday(dt)


#         print('%s, %s' % (dt.isoformat(), txt))

#     def __init__(self):
#         # Keep a reference to the "close" line in the data[0] dataseries
#         self.dataclose = self.datas[0].close
#         self.dataopen = self.datas[0].open
#         self.dt = self.datas[0].datetime.date(0)
#         self.dt_time = self.datas[0].datetime.time(0)
#         self.dt_weekday = datetime.weekday(self.dt)

#     def notify_order(self, order):
#         if order.status in [order.Submitted, order.Accepted]:
#             # Buy/Sell order submitted/accepted to/by broker - Nothing to do
#             return

#         # Check if an order has been completed
#         # Attention: broker could reject order if not enougth cash
#         if order.status in [order.Completed]:
#             if order.isbuy():
#                 self.log(
#                     'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, Datetime: %s' %
#                     (order.executed.price,
#                      order.executed.value,
#                      order.executed.comm,
#                     bt.num2date(order.executed.dt)))

#                 self.buyprice = order.executed.price
#                 self.buycomm = order.executed.comm
#             else:  # Sell
#                 self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, Datetime: %s' %
#                          (order.executed.price,
#                           order.executed.value,
#                           order.executed.comm,
#                           bt.num2date(order.executed.dt)))

#             self.bar_executed = len(self)

#         elif order.status in [order.Canceled, order.Margin, order.Rejected]:
#             self.log('Order Canceled/Margin/Rejected')

#         self.order = None

#     def notify_trade(self, trade):
#         if not trade.isclosed:
#             return
#         self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
#                  (trade.pnl, trade.pnlcomm))
        
#     def start(self):
#         self.val_start = self.broker.get_cash()  # keep the starting cash

#     def nextstart(self):
#         self.buy(size = 100)
#         print(self.broker.getposition(data))
    
#     def next(self):
#         self.close(size = 100)
#         print(self.broker.getposition(data))

#     def stop(self):
#         print(self.broker.get_orders_open(True))
#         # print(self.broker.getposition(data))
#         self.val_end = self.broker.get_cash()
#         # print(self.broker.get_value())
#         # print(self.val_start)
#         # print(self.val_end)
#         self.roi = (self.val_end - self.val_start)/100
#         print('ROI:  {:.2f}%'.format(self.roi))

if __name__ == '__main__':
    # Create a cerebro instance, add our strategy, some starting cash at broker and a 0.1% broker commission
    cerebro = bt.Cerebro()
    cerebro.addstrategy(InAndOut)
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.adddata(data)
    print('<START> Brokerage account: $%.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('<FINISH> Brokerage account: $%.2f' % cerebro.broker.getvalue())
    # Plot the strategy
    # cerebro.plot(style='candlestick',loc='grey', grid=False) #You can leave inside the paranthesis empty



