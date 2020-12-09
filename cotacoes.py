import statistics as st
import pandas as pd
import numpy as np
import math
import functools


# @pd.api.extensions.register_dataframe_accessor('cotacoes')
class Cotacoes:
    def __init__(self, serie, *args, **kwargs):
        """
        n_classes: Numero de classes para tabela de classes
        """
        self.serie = serie
        self.n_classes = kwargs.pop('n_classes', 10) # numero de classes
        self._n_classes = np.round(math.pow(self.amplitude, 1/2))
    
    @property
    def n_classes(self):
        return self._n_classes
    
    @n_classes.setter
    def n_classes(self, value):
        self._n_classes = value
    
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
            .groupby(pd.cut(self.serie['Close'], bins=10)) #classes))
            .sum()
            .drop(columns=['Close']))
        
        _s['Rel Volume'] = _s['Volume'] / self.volumetotal * 100
        _s['Cum Volume'] = _s['Volume'].cumsum()
        _s['Cum Rel Volume'] = _s['Cum Volume'] / self.volumetotal * 100
        
        return _s
    
    @functools.cached_property
    def tabela_classes_volume(self):
        _svol = (self.serie['Volume'] / 1000000).round(decimals=0).astype(int)

        amplitude_classe = np.ceil((_svol.max() - _svol.min()) / self.n_classes)
        
        classes = np.arange(_svol.min(),
                _svol.max() + _svol.max() % amplitude_classe, amplitude_classe)
        
        _c = iter(classes); next(_c)
        
        # labels = ['%s ⊣ %s' % (left, right) for left, right in zip(classes, _c)]
        labels = None
        
        _svol = _svol.groupby(pd.cut(_svol, classes, labels=labels)).count()
                
        return _svol

    
    @functools.cached_property
    def tabela_classes_preco(self):
        coluna = self.serie[self.serie['Open'] != 0.0]['Open']
        _s = pd.DataFrame(coluna.groupby(pd.cut(coluna, bins=10, precision=2))
                          .count())
        return _s