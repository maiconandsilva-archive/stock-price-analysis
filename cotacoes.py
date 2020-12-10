import statistics as st
import pandas as pd
import numpy as np
import math
import functools


@pd.api.extensions.register_dataframe_accessor('analise')
class Cotacoes:
    """
    serie: pd.Dataframe
        Cotações de um determinado periodo de um instrumento financeiro.
        Espera-se que a variável serie tenha as seguintes colunas:
            - Volume    -> Número de negócios no dado período
            - Open      -> Cotação na abertura do pregão
            - Close     -> Cotação no fechamento do pregão
            - Adj Close -> Cotação de ajuste de cada pregão
            - High      -> Maior cotação no dado período
            - Low       -> Menor cotação no dado período
        
    n_classes: int
        Numero de classes para tabela de classes
        default: √serie.size (raiz quadrada do número de dados da série)
    """
    
    def __init__(self, serie):
        self._serie = self.__normalizar(serie)
        
        # Padrão para o número de classes
        # Segundo recomendacao material Prof. Lapponi
        self._nclasses = np.round(math.pow(len(self._serie), 0.5))
    
    def __normalizar(self, serie):
        return serie
    
    @property
    def nclasses(self):
        return self._nclasses
    
    @nclasses.setter
    def nclasses(self, nclasses):
        self._nclasses = nclasses
    
    @functools.cached_property
    def volumetotal(self):
        "Volume de negociacoes no instrumento financeiro"
        return self._serie['Volume'].sum()

    @functools.cached_property
    def amplitude_serie(self):
        """
        Diferenca entre a maior e a menor cotacao do ano.
        """
        return self.maxima('Open') - self.minima('Close')    
    
    @functools.lru_cache
    def minima(self, coluna):
        "Menor cotacao negociada na serie do ano"
        return self._serie[coluna].min()
    
    @functools.lru_cache
    def maxima(self, coluna):
        """
        Maior cotacao negociada na serie do ano, seja abertura, fechamento
        """
        return self._serie[coluna].max()
     
    @functools.lru_cache
    def amplitude(self, coluna):
        """
        Diferenca entre a maior e a menor cotacao ou volume do ano.
        """
        return self.maxima(coluna) - self.minima(coluna)
    
    @functools.lru_cache
    def tabela_classes_cotacoes(self, cotacao):
        coluna = self._serie[self._serie[cotacao] != 0.0][cotacao]
        return pd.DataFrame(coluna.groupby(
            pd.cut(coluna, bins=self.nclasses, precision=2)).count())    
    
    @functools.cached_property
    def tabela_classes(self):
        amplitude_classe = self.amplitude_serie / self.nclasses
        
        classes = np.arange(
            self.minima('Low'), self.maxima('High') + amplitude_classe, amplitude_classe)
        
        _s = (self._serie
            .drop(columns=['Adj Close', 'High', 'Low', 'Open'])
            .groupby(pd.cut(self._serie['Close'], bins=self.nclasses)) #classes))
            .sum()
            .drop(columns=['Close']))
        
        _s['Rel Volume'] = _s['Volume'] / self.volumetotal * 100
        _s['Cum Volume'] = _s['Volume'].cumsum()
        _s['Cum Rel Volume'] = _s['Cum Volume'] / self.volumetotal * 100
        
        return _s
    
    @functools.cached_property
    def tabela_classes_volume(self):
        _s = (self._serie['Volume'] / 100000).round(decimals=0).astype(int)

        amplitude_classe = np.ceil((_s.max() - _s.min()) / self.nclasses)
        
        classes = np.arange(_s.min(),
                _s.max() + _s.max() % amplitude_classe, amplitude_classe)
        
        # _c = iter(classes); next(_c)
        
        # labels = ['%s ⊣ %s' % (left, right) for left, right in zip(classes, _c)]
        labels = None
        
        _s = _s.groupby(pd.cut(_s, classes, labels=labels)).count()
                
        return _s

    @functools.lru_cache
    def cotacao_agrupada_por_periodo(self, periodo):
        _s = self._serie.resample(periodo).agg({
            'Open': 'first',
            'Close': 'last',
            'High': 'max',
            'Low': 'min',
            'Volume': 'sum',
        })
        return _s
    
    @functools.lru_cache
    def diferenca_mesmo_periodo(self, coluna1, coluna2, nome=''):
        _s = self._serie.copy()
        _s[nome or f'{coluna1} - {coluna2}'] = _s[coluna1] - _s[coluna2]
        return _s
    
    @functools.lru_cache
    def diferenca_periodos_distintos(self, coluna1, coluna2, nome=''):
        _s = self._serie.copy()
        _sdiff = self._serie[1:].copy()
        
        _s[nome or f'{coluna1} - {coluna2}'] = _s[coluna1] - _sdiff[coluna2]
        return _s