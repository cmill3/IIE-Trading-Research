import pandas as pd
import numpy as np
from datetime import datetime
import json
import pandas as pd
import numpy as np
from datetime import datetime
import json

class OptionsBacktester:
    def __init__(self, initial_capital=100000, risk_allotment=0.005):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_allotment = risk_allotment  # Risk n% of capital per trade
        self.open_contracts_value = 0  # Value of open contracts
        self.positions = {}  # Current open positions
        self.trades_history = []  # Complete history of trades
        self.portfolio_history = []  # Track portfolio value over time
        self.hourly_history = []  # Track hourly performance
        
    def _parse_json_string(self, json_str):
        """Convert string representation of JSON to dictionary."""
        if isinstance(json_str, str):
            try:
                return json.loads(json_str.replace("'", '"'))
            except json.JSONDecodeError:
                # If JSON parsing fails, try evaluating as Python literal
                import ast
                try:
                    return ast.literal_eval(json_str)
                except:
                    print(f"Warning: Could not parse JSON string: {json_str}")
                    return {}
        return json_str
        
    def process_trades(self, df):
        """Main method to process all trades in the dataset."""
        # Sort DataFrame by trade date to ensure chronological processing
        df = df.sort_values('open_trade_dt')
        
        # Initialize tracking variables
        current_date = None
        current_hour = None
        current_portfolio_value = self.initial_capital
        value_contracts_purchased = 0
        daily_stats = {
            'date': None,
            'portfolio_value': self.initial_capital,
            'open_positions': 0,
            'daily_pnl': 0,
            'daily_value_contracts_purchased': 0
        }
        hourly_stats = {
            'date': None,
            'hour': None,
            'live_pnl': 0,
            'hourly_positions': 0,
            'total_portfolio_value': self.initial_capital,
            'cash_value': self.initial_capital,
            'value_contracts_open': 0
        }
        open_times = df['open_trade_dt'].values
        close_times = df['close_trade_dt'].values
        ## create a list of all the unique values in the open and close times
        all_trading_times = np.unique(np.concatenate([open_times, close_times]))

        for trading_time in all_trading_times:
            ## get all the trades that have the same trading time
            open_trades = df[df['open_trade_dt'] == trading_time]
            for _, row in open_trades.iterrows():
                try:
                    # Convert string timestamps to datetime objects
                    open_dt = pd.to_datetime(row['open_trade_dt'])
                    close_dt = pd.to_datetime(row['close_trade_dt'])
                    
                    # Process opening trade
                    buy_info = self._parse_json_string(row['buy_info'])
                    if not buy_info:
                        print(f"Skipping trade due to invalid buy_info: {row['position_id']}")
                        continue
                    
                    position_cost = self._open_position(
                        row=row,
                        entry_price=float(buy_info['open_price']),
                        symbol=row['option_symbol'],
                        timestamp=open_dt,
                        contract_type=buy_info.get('contract_type', 'unknown'),
                        close_dt=close_dt,
                        sell_info=row['sell_info']
                    )
                    value_contracts_purchased += position_cost

                except Exception as e:
                    print(f"Error processing trade {row['position_id']}: {str(e)}")
                    continue

            open_positions = pd.DataFrame(self.positions).T
            open_positions['close_dt'] = pd.to_datetime(open_positions['close_dt'])
            trades_to_close = open_positions[open_positions['close_dt'] == trading_time]

            for trade_id, row in trades_to_close.iterrows():
                try:
                    # Process closing trade
                    sell_info = self._parse_json_string(row['sell_info'])
                    if not sell_info:
                        print(f"Skipping trade close due to invalid sell_info: {row['position_id']}")
                        continue
                    
                    # Handle the case where close_price might be a string
                    try:
                        exit_price = float(sell_info['close_price'])
                    except (ValueError, TypeError):
                        print(f"Invalid close price for position: {row['position_id']}")
                        continue
                    
                    self._close_position(
                        position_id=trade_id,
                        exit_price=exit_price,
                        timestamp=close_dt,
                        sell_code=sell_info.get('sell_code', 'unknown')
                    )
                except Exception as e:
                    print(f"Error processing trade {row['position_id']}: {str(e)}")
                    continue
                
            try:
                ## update hourly statistics
                trade_hour = open_dt.hour
                if current_hour != trade_hour:
                    if current_hour is not None:
                        self.hourly_history.append(hourly_stats.copy())
                    current_hour = trade_hour
                    live_pnl = (self.current_capital + self._calculate_open_positions_value()) - current_portfolio_value
                    hourly_stats = {
                        'date': open_dt.date(),
                        'hour': current_hour,
                        'live_pnl': live_pnl,
                        'hourly_positions': len(self.positions),
                        'total_portfolio_value': self.current_capital + self._calculate_open_positions_value(),
                        'cash_value': self.current_capital,
                        'value_contracts_open': self._calculate_open_positions_value()
                    }

                # Update daily statistics if we're on a new day
                trade_date = open_dt.date()
                if current_date != trade_date:
                    if current_date is not None:
                        self.portfolio_history.append(daily_stats.copy())
                    current_date = trade_date
                    end_of_day_capital = self.current_capital + self._calculate_open_positions_value()
                    daily_pnl = end_of_day_capital - current_portfolio_value
                    current_portfolio_value = end_of_day_capital
                    daily_stats = {
                        'date': current_date,
                        'portfolio_value': end_of_day_capital,
                        'open_positions': len(self.positions),
                        'daily_pnl': daily_pnl,
                        'daily_value_contracts_purchased': value_contracts_purchased
                    }
                    value_contracts_purchased = 0
            except Exception as e:
                print(f"Error processing trade {row['position_id']}: {str(e)}")
                continue
        # Add final day's stats
        self.portfolio_history.append(daily_stats)
        
    def _open_position(self, row, entry_price, symbol, timestamp, contract_type, close_dt, sell_info):
        """Open a new position."""
        quantity = self._compute_quantity(self.current_capital, entry_price)
        position_cost = entry_price * quantity * 100  # Options contracts are for 100 shares
        trade_id = row['position_id']
        
        if self.current_capital >= position_cost:
            self.positions[trade_id] = {
                'entry_price': entry_price,
                'quantity': quantity,
                'symbol': symbol,
                'entry_time': timestamp,
                'contract_type': contract_type,
                'cost_basis': position_cost,
                'close_dt': close_dt,
                'sell_info': sell_info
            }
            
            self.current_capital -= position_cost
            
            self.trades_history.append({
                'timestamp': timestamp,
                'action': 'BUY',
                'position_id': trade_id,
                'price': entry_price,
                'quantity': quantity,
                'symbol': symbol,
                'capital_after': self.current_capital
            })
            
            return position_cost
        else:
            return 0
        

    def _update_open_positions_value(self):
        """Update the value of all open positions."""
        self.open_contracts_value = self._calculate_open_positions_value()

    def _compute_quantity(self, capital, price):
        """Compute the number of contracts to buy based on capital, risk allotment and price."""
        trade_budget = capital * self.risk_allotment
        if trade_budget > 1500:
            trade_budget = 1500
        return int(trade_budget / (price * 100))
    
    def _close_position(self, position_id, exit_price, timestamp, sell_code):
        """Close an existing position."""
        if position_id in self.positions:
            position = self.positions[position_id]
            exit_value = exit_price * position['quantity'] * 100
            
            # Calculate P&L
            pnl = exit_value - position['cost_basis']
            
            self.current_capital += exit_value
            
            self.trades_history.append({
                'timestamp': timestamp,
                'action': 'SELL',
                'position_id': position_id,
                'price': exit_price,
                'quantity': position['quantity'],
                'symbol': position['symbol'],
                'pnl': pnl,
                'sell_code': sell_code,
                'capital_after': self.current_capital
            })
            
            del self.positions[position_id]
    
    def _calculate_open_positions_value(self):
        """Calculate the current value of all open positions."""
        return sum(pos['quantity'] * pos['entry_price'] * 100 for pos in self.positions.values())
    
    def get_performance_metrics(self):
        """Calculate and return key performance metrics."""
        if not self.trades_history:
            return None
            
        trades_df = pd.DataFrame(self.trades_history)
        trades_df.to_csv('trades.csv')
        portfolio_df = pd.DataFrame(self.portfolio_history)
        portfolio_df.to_csv('portfolio.csv')
        hourly_df = pd.DataFrame(self.hourly_history)
        hourly_df.to_csv('hourly.csv')
        
        # Calculate metrics
        total_trades = len(trades_df[trades_df['action'] == 'SELL'])
        profitable_trades = len(trades_df[(trades_df['action'] == 'SELL') & (trades_df['pnl'] > 0)])
        total_pnl = trades_df[trades_df['action'] == 'SELL']['pnl'].sum()
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0
        
        # Calculate returns
        final_capital = self.current_capital + self._calculate_open_positions_value()
        total_return = (final_capital - self.initial_capital) / self.initial_capital * 100
        
        return {
            'total_trades': total_trades,
            'profitable_trades': profitable_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'total_return_pct': total_return,
            'final_capital': final_capital
        }

# Example usage
def run_backtest(df, initial_capital=100000):
    """Run backtest on the provided DataFrame."""
    backtester = OptionsBacktester(initial_capital=initial_capital)
    backtester.process_trades(df)
    
    metrics = backtester.get_performance_metrics()
    return backtester, metrics

# Function to analyze results
def analyze_results(backtester, metrics):
    """Print detailed analysis of backtest results."""
    print("\n=== Backtest Results ===")
    print(f"Initial Capital: ${backtester.initial_capital:,.2f}")
    print(f"Final Capital: ${metrics['final_capital']:,.2f}")
    print(f"Total Return: {metrics['total_return_pct']:.2f}%")
    print(f"\nTotal Trades: {metrics['total_trades']}")
    print(f"Profitable Trades: {metrics['profitable_trades']}")
    print(f"Win Rate: {metrics['win_rate']*100:.2f}%")
    print(f"Total P&L: ${metrics['total_pnl']:,.2f}")


if __name__ == "__main__":
    # Load sample data
    df = pd.read_csv('/Users/charlesmiller/Documents/Code/IIE/research_resources/APE-Research/APE-Backtester/inv_backtesters/top_trades_3.csv')
    df = df.loc[df['symbol'].isin(['NVDA', 'TSLA', 'SPY', 'QQQ', 'AAPL','MSFT'])]
    df['position_id'] = df.apply(lambda x: f"{x['position_id']}_{x['order_id']}", axis=1)
    ## drop duplicates based on the position_id
    df = df.drop_duplicates(subset='position_id')
    # Run backtest
    backtester, metrics = run_backtest(df, initial_capital=125000)
    
    # Analyze results
    analyze_results(backtester, metrics)