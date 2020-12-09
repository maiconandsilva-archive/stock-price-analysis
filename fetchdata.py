import pandas_datareader as web


def buscar_cotacoes_yahoo(ticker, periodo, /):
    """ Busca cotações a partir de tickers
        de cotacoes financeiros (Ações, índices etc)
        no site https://finance.yahoo.com
    """
    return web.DataReader(ticker, 'yahoo', periodo['inicio'], periodo['fim'])