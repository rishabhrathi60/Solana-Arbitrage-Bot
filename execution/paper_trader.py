from datetime import datetime

from database.trades import create_database, get_all_trades, save_trade


create_database()


def paper_trade(opportunity):
    trade = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "buy": opportunity["buy"],
        "sell": opportunity["sell"],
        "starting_amount": 1.00,
        "ending_amount": opportunity["ending_amount"],
        "profit": opportunity["profit"],
        "decision": opportunity["decision"],
    }

    save_trade(trade)

    return trade


def get_trade_history():
    return get_all_trades()