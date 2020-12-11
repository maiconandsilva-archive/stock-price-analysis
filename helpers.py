import pandas as pd
import functools


def tabela(id='', tabelas_reg=None):
    """Adiciona um DataFrame ao registro de tabelas"""
    def outter_wrapper(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            nonlocal tabelas_reg
            _tabelaid = kwargs.pop('tabelaid', None) or id or f.__name__
            
            tabelas_reg = tabelas_reg or args[0]._tabelas_reg # arg[0] == self
            tabela = tabelas_reg.get(_tabelaid)
            tabelas_reg[_tabelaid] = f(*args, tabela=tabela, **kwargs)
            
            return tabelas_reg[_tabelaid]
        return wrapper
    return outter_wrapper


def coluna(id='', tabelaid='', tabelas_reg=None):
    """Adiciona uma coluna a um dataframe. Se o DataFrame não existe, cria um."""
    def outter_wrapper(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            nonlocal tabelas_reg
            tabelas_reg = tabelas_reg or args[0]._tabelas_reg # arg[0] == self
            
            _tabelaid = kwargs.pop('tabelaid', tabelaid) or f.__name__
            tabela = tabelas_reg.get(_tabelaid)
            tabela = tabela if tabela is not None else pd.DataFrame()
            
            coluna = f(*args, tabela=tabela, **kwargs)
            _colunaid = kwargs.pop('colunaid', None) or id or coluna.name
            _colvar_f = args if args and isinstance(args[0], str) else args[1:]
            _colunaid = _colunaid.format(*_colvar_f)
            
            if not isinstance(coluna, pd.DataFrame):
                tabela[_colunaid] = coluna
                
            tabelas_reg[_tabelaid] = tabela
            return tabelas_reg[_tabelaid]
        return wrapper
    return outter_wrapper


def verificar_tabela(tabela, sentinela):
    return sentinela if tabela is None or tabela.empty else tabela

def keepcolumns(df, colunas):
    """Usa apenas as colunas dadas como argumento."""
    return df.columns.difference(colunas)

def make_classes(parameter_list):
    """
    docstring
    """
    pass
    
def prettify_labels(labels):
    """
    docstring
    """    
    pass