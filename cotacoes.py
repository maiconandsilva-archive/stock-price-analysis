import statistics as st
import pandas as pd
import numpy as np
import functools
import pandas_datareader as web


def buscar_cotacoes(ticker, periodo, /):
    """ Busca cotações a partir de tickers
        de cotacoes financeiros (Ações, índices etc)
        no site https://finance.yahoo.com
    """
    return web.DataReader(ticker, 'yahoo', periodo['inicio'], periodo['fim'])


class Cotacoes:
    def __init__(self, serie, *args, **kwargs):
        """
        n_classes: Numero de classes para tabela de classes
        """
        self.serie = serie
        self.n_classes = kwargs.pop('n_classes', 10) # numero de classes
    
    @functools.cached_property
    def minima(self):
        "Menor cotacao negociada na serie do ano"
        return self.serie["Low"].min()
     
    @functools.cached_property
    def maxima(self):
        "Maior cotacao negociada na serie do ano"
        return self.serie["High"].max()
     
    @functools.cached_property
    def amplitude(self):
        """
        Diferenca entre a maior e a menor cotacao do ano.
        """
        return self.maxima - self.minima
    
    @functools.cached_property
    def volumetotal(self):
        "Volume de negociacoes no instrumento financeiro"
        return self.serie['Volume'].sum()
    
    @functools.cached_property
    def tabela_classes(self):
        amplitude_classe = self.amplitude / self.n_classes
        
        classes = np.arange(
            self.minima, self.maxima + amplitude_classe, amplitude_classe)
        
        _s = (self.serie
            .drop(columns=['Adj Close', 'High', 'Low', 'Open'])
            .groupby(pd.cut(self.serie['Close'], classes))
            .sum()
            .drop(columns=['Close']))
        
        _s['Rel Volume'] = _s['Volume'] / self.volumetotal * 100
        _s['Cum Volume'] = _s['Volume'].cumsum()
        _s['Cum Rel Volume'] = _s['Cum Volume'] / self.volumetotal * 100
        
        return _s