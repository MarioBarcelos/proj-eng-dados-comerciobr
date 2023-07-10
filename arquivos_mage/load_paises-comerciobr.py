import pandas as pd
import urllib
import requests
import json
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
    
    with urllib.request.urlopen("https://storage.googleapis.com/comerciobr2122/paises.json") as urlpaises:
        paises = json.load(urlpaises)
    
    return paises


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

