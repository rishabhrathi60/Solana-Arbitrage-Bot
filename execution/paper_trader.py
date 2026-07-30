from datetime import datetime


trade_history = []


def paper_trade(opportunity):
    trade = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "buy": opportunity["buy"],
        "sell": opportunity["sell"],
        "profit": opportunity["profit"],
        "decision": opportunity["decision"],
    }

    trade_history.append(trade)

    return trade


def get_trade_history():
    return trade_history