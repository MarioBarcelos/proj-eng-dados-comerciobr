import pandas as pd
import requests
import io

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    exp22 = requests.get('https://storage.googleapis.com/comerciobr2122/EXP_2022_MUN.csv')

    return pd.read_csv(io.StringIO(exp22.text), names = ['Ano', 'Mes', 'Produtos', 'Pais(dest)', 'Estado', 
                                                         'Municipio','KG_Liq', 'Total_em_Dólar'], sep = ';',
                                                         encoding="utf-8-sig", low_memory=True)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
