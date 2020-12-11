import statistics as st
import pandas as pd
import numpy as np
import math
import functools
import helpers


@pd.api.extensions.register_dataframe_accessor('analise')
class Cotacoes:
    """
    serie: pd.Dataframe
        Cotações de um determinado periodo de um instrumento financeiro.
        Espera-se que a variável serie tenha as seguintes colunas:
            - Volume    -> Quantidade de títulos negócios no dado período
            - Open      -> Cotação da abertura do dado período
            - Close     -> Cotação do fechamento do dado período
            - Adj Close -> Cotação de ajuste de cada dado período
            - High      -> Maior cotação no dado período
            - Low       -> Menor cotação no dado período
        
    n_classes: int
        Número de classes para tabela de classes
        default: √serie.size (raiz quadrada do número de dados da série)
        
    _tabelas_reg: dict
        Registro de tabelas geradas. Pode funcionar como cache.
    """
    
    _tabelas_reg = {}
    
    def __init__(self, serie):
        self._serie = self.__normalizar(serie)
        
        # Padrão para o número de classes
        # Segundo recomendacao material Prof. Lapponi
        self._nclasses = int(np.round(math.pow(len(self._serie), 0.5)))
    
    def __normalizar(self, serie):
        return serie
    
    @property
    def nclasses(self):
        return self._nclasses
    
    @nclasses.setter
    def nclasses(self, nclasses):
        self._nclasses = nclasses

    def cache(self, tabelaid):
        return self._tabelas_reg[tabelaid]   
    
    @functools.cached_property
    def volumetotal(self):
        """Volume de negociacoes no instrumento financeiro"""
        return self._serie['Volume'].sum()

    @functools.cached_property
    def amplitude_serie(self):
        """Diferenca entre a maior e a menor cotacao da série."""
        return self.maxima('High') - self.minima('Low')    
    
    @functools.lru_cache
    def minima(self, coluna):
        """Menor cotacao negociada na serie do dado período."""
        return self._serie[coluna].min()
    
    @functools.lru_cache
    def maxima(self, coluna):
        """Maior cotacao negociada na serie do dado período."""
        return self._serie[coluna].max()
     
    @functools.lru_cache
    def amplitude(self, coluna):
        """
        Diferenca entre a maior e a menor cotacao ou volume do dado período.
        """
        return self.maxima(coluna) - self.minima(coluna)

    @helpers.coluna('{0}-{1}')
    def diferenca_periodo(self, col1, col2, periodos_consecutivos=False, **kwargs):
        _s = helpers.verificar_tabela(kwargs.get('tabela'), self._serie)
        return (_s[col1].shift() if periodos_consecutivos else _s[col1]) - _s[col2]
        
    @functools.lru_cache
    def tabela_classes_cotacoes(self, cotacao, **kwargs):
        coluna = self._serie[self._serie[cotacao] != 0.0][cotacao]
        return pd.DataFrame(coluna.groupby(
            pd.cut(coluna, bins=self.nclasses, precision=2)).count())     
    
    @helpers.tabela()
    def tabela_classes(self, **kwargs):
        # amplitude_classe = self.amplitude_serie / self.nclasses
        # classes = np.arange(
        #     self.minima('Low'), self.maxima('High') + amplitude_classe, amplitude_classe)
        
        _s = (self._serie
            .drop(columns=helpers.keepcolumns(self._serie, ['Close', 'Volume']))
            .groupby(pd.cut(self._serie['Close'], bins=self.nclasses)) #classes))
            .sum()
            .drop(columns=['Close']))
           
        return _s
    
    @helpers.tabela()
    def colunas_frequencias(self, coluna, **kwargs):
        _s = helpers.verificar_tabela(kwargs.get('tabela'), self._serie)
        total = _s[coluna].sum()
        _s[f'Rel {coluna}'] = _s[coluna] / total  * 100
        _s[f'Cum {coluna}'] = _s[coluna].cumsum()
        _s[f'Cum Rel {coluna}'] = _s[f'Cum {coluna}'] / total * 100
        return _s
    
    @helpers.coluna('Volume Hund Thous')
    def agrupamento_volume(self, **kwargs):
        _s = (self._serie['Volume'] / 100000).round(decimals=0).astype(int)

        # amplitude_classe = np.ceil((_s.max() - _s.min()) / self.nclasses)
        
        # classes = np.arange(_s.min(),
        #         _s.max() + _s.max() % amplitude_classe, amplitude_classe)
        
        # _c = iter(classes); next(_c)
        
        # labels = ['%.2f ⊣ %.2f' % (left, right) for left, right in zip(classes, _c)]
        # labels = None
        
        _s = _s.groupby(pd.cut(_s, bins=self.nclasses)).count() # classes, labels=labels)).count()
                
        return _s

    @helpers.tabela()
    def cotacao_agrupada_por_periodo(self, periodo, **kwargs):
        _s = self._serie.resample(periodo).agg({
            'Open': 'first',
            'Close': 'last',
            'High': 'max',
            'Low': 'min',
            'Volume': 'sum',
        })
        return _s
    