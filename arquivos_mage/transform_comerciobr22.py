import pandas as pd 
import numpy as np 
from sklearn.feature_extraction.text import CountVectorizer
import csv 
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(dadosexp22,meses,produtos,paises,estados,municipios, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    ##################################
    # Limpeza e Tratamento dos Dados #
    ##################################

    # Altero o tipo de dados das colunas para utilização com o map
    dadosexp22["Mes"] = dadosexp22["Mes"].astype('string')
    dadosexp22["Produtos"] = dadosexp22["Produtos"].astype('string')
    dadosexp22["Pais(dest)"] = dadosexp22["Pais(dest)"].astype('string')
    dadosexp22["Estado"] = dadosexp22["Estado"].astype('string')
    dadosexp22["Municipio"] = dadosexp22["Municipio"].astype('string')

    # Com a função drop removo a coluna "ano" e a linha "0".
    dadosexp22.drop(["Ano"], axis=1, inplace=True)
    dadosexp22.drop([0], inplace=True)

    # Por fim faço a remoção de duplicatas e valores NA
    dadosexp22 = dadosexp22.drop_duplicates()
    dadosexp22 = dadosexp22.dropna()

    # Renomeio todas as colunas abaixo utilizando como valor de saída as Nomeclaturas originais
    dadosexp22["Mes"] = dadosexp22["Mes"].map(meses,na_action='ignore')
    dadosexp22["Produtos"] = dadosexp22["Produtos"].map(produtos,na_action='ignore')
    dadosexp22["Pais(dest)"] = dadosexp22["Pais(dest)"].map(paises,na_action='ignore')
    dadosexp22["Estado"] = dadosexp22["Estado"].map(estados,na_action='ignore')
    dadosexp22["Municipio"] = dadosexp22["Municipio"].map(municipios,na_action='ignore')

    # Seleciono o período a ser analisado com a função loc (Jan,Fev,Mar,Abril,Maio,Junho) 
    dadosexp22 = dadosexp22.loc[dadosexp22['Mes'].isin(['Janeiro','Fevereiro','Março',
                                                        'Abril','Maio','Junho','Julho',
                                                        'Agosto','Setembro','Outubro',
                                                        'Novembro','Dezembro'])]

    # Incluo o 'index' do pandas como propriedade do DF e reordeno o DF
    dadosexp22["Id_Venda"] = dadosexp22.index
    dadosexp22 = dadosexp22[['Id_Venda','Municipio','Estado','Pais(dest)','Produtos',
                             'Total_em_Dólar','KG_Liq','Mes']]

    ###################################################################################
    # Informações sobre a receita 'bruta' total obtida com as Exportações Brasileiras #
    # (Brasil -> Comércio Exterior) e o total de operações. ###########################
    ###################################################################################

    # Altero o tipo das colunas que serão utilizadas
    dadosexp22['Total_em_Dólar'] = dadosexp22['Total_em_Dólar'].astype('int')

    # Calculando os valores das importações 2022
    expor_total22 = dadosexp22["Total_em_Dólar"].sum()
    expor_count22 = dadosexp22["Total_em_Dólar"].count()
    expor_media22 = dadosexp22["Total_em_Dólar"].mean()

    # Crio um dataframe utilizando o "pandas"
    exportacoes22 = pd.DataFrame({"Ano": ["2022"],
                                  "Total_de_Transacoes": expor_count22,
                                  "Receita_Total_Exportacoes": expor_total22,
                                  "Valor_medio_das_Exportacoes": expor_media22})
    # Incluo uma coluna no meu DF com passando o 'Index'
    exportacoes22["exportacoes22_id"] = exportacoes22.index

    # Edito a formatação numérica das colunas 
    exportacoes22 ["Total_de_Transacoes"] = exportacoes22["Total_de_Transacoes"].map("{:,.0f}".format)
    exportacoes22 ["Receita_Total_Exportacoes"] = exportacoes22["Receita_Total_Exportacoes"].map("${:,.0f}".format)
    exportacoes22 ["Valor_medio_das_Exportacoes"] = exportacoes22["Valor_medio_das_Exportacoes"].map("${:,.0f}".format)

    # Faço uma 'Reordenação' do DF
    exportacoes22 = exportacoes22[['exportacoes22_id','Ano','Total_de_Transacoes',
                                   'Receita_Total_Exportacoes','Valor_medio_das_Exportacoes']]

    ########################################################################################################
    # Faço um resumo, individual, da receita 'bruta' obtida com as Exportações por cada Estado Brasileiro. #
    ########################################################################################################

    # Inicio agrupando os valores/operações de cada estado e realizo os devidos calculos
    uf_total22 = dadosexp22.groupby('Estado')['Total_em_Dólar'].sum()
    uf_media22 = dadosexp22.groupby("Estado")["Total_em_Dólar"].mean()
    uf_operacoes22 = dadosexp22.groupby("Estado")["Total_em_Dólar"].count()

    # Crio um DataFrame para armazenar os Valores agrupados
    uf_analises22 = pd.DataFrame({"Numero_de_Transacoes" : uf_operacoes22,
                                  "Valor_medio_das_Exportacoes" : uf_media22,
                                  "Valor_total_das_Exportacoes" : uf_total22})

    #  Incluo uma coluna no meu DF com passando o 'Index'
    uf_analises22["Uf_analises22_id"] = uf_analises22.index

    # Edito a formatação numérica dos dados
    uf_analises22 = uf_analises22.round(2)
    uf_analises22 ["Valor_medio_das_Exportacoes"] = uf_analises22["Valor_medio_das_Exportacoes"].map("${:,.2f}".format)
    uf_analises22 ["Valor_total_das_Exportacoes"] = uf_analises22["Valor_total_das_Exportacoes"].map("${:,.2f}".format)

    # Faço uma 'Reordenação' do DF
    uf_analises22 = uf_analises22[['Uf_analises22_id','Numero_de_Transacoes','Valor_total_das_Exportacoes',
                                   'Valor_medio_das_Exportacoes']]

    ######################################################################################################
    # Detalhamento individual dos valores arrecadados por cada Munícipio/Sede das Empresas Exportadoras. #
    ######################################################################################################

    # Inicio agrupando os valores/operações de cada estado e realizo os devidos calculos
    mun_total22 = dadosexp22.groupby("Municipio")["Total_em_Dólar"].sum()
    mun_media22 = dadosexp22.groupby("Municipio")["Total_em_Dólar"].mean()
    mun_operacoes22 = dadosexp22.groupby("Municipio")["Total_em_Dólar"].count()

    # Criando um DataFrame dos Municípios com os valores agrupados
    mun_analises22 = pd.DataFrame({"Numero_de_Vendas_para_o_Exterior" : mun_operacoes22,
                                   "Valor_medio_das_Exportacoes" : mun_media22,
                                   "Valor_total_das_Exportacoes" : mun_total22})

    #  Incluo uma coluna no meu DF com passando o 'Index'
    mun_analises22["Mun_analises22_id"] = mun_analises22.index

    # Edito a formatação numérica dos dados
    mun_analises22 ["Valor_total_das_Exportacoes"] = mun_analises22["Valor_total_das_Exportacoes"].map("${:,.0f}".format)
    mun_analises22 ["Valor_medio_das_Exportacoes"] = mun_analises22["Valor_medio_das_Exportacoes"].map("${:,.0f}".format)

    # Faço uma 'Reordenação' do DF
    mun_analises22 = mun_analises22[["Mun_analises22_id","Numero_de_Vendas_para_o_Exterior",
                                     "Valor_total_das_Exportacoes", "Valor_medio_das_Exportacoes"]]

    ####################################################
    # Principais destinos das Exportações Brasileiras. #
    ####################################################

    model_22 = dadosexp22["Pais(dest)"]
    model_22.index = range(0, 994112, 1)

    destinos_22 = [str(i) for i in model_22]

    # Criando um vetor
    vetor_destinos22 = CountVectorizer(analyzer = 'word', lowercase=False)

    # Vetorizando o dataset
    destinos22 = vetor_destinos22.fit_transform(destinos_22)

    # Retornando paises únicos
    destinos_unicos22 = vetor_destinos22.get_feature_names_out()

    # Criar um novo DataFrame dos Paises
    dfdestinos22 = pd.DataFrame(destinos22.todense(), columns = destinos_unicos22, index = model_22.index)

    # Calcular o percentual 
    porcent_destinos22 = 100 * pd.Series(dfdestinos22.sum()).sort_values(ascending = False) / dfdestinos22.shape[0]

    # Faço a 'limpeza' de 'Dados' reconhecidos com 'nan'
    porcent_destinos22.drop(["nan"], inplace=True)

    # Crio um DF para melhor aproveitamento dos Dados
    porcentagem_dest22 = pd.DataFrame({'Destinos22':porcent_destinos22})

    # Incluo o 'Index' como propriedade do DF
    porcentagem_dest22['Destinos22_id'] = porcentagem_dest22.index

    # Reordeno o DF
    porcentagem_dest22 = porcentagem_dest22[['Destinos22_id','Destinos22']]

    #######################################################################################
    # Abaixo localizo os produtos mais requisitados por importadoras no Mercado Exterior. #
    #######################################################################################

    # Agrupo os Produtos e realizo a sua Contagem, em seguida, Somo os Valores de todas as suas Compras
    produtos_contagem22 = dadosexp22.groupby("Produtos")["Total_em_Dólar"].count()
    produtos_soma22 = dadosexp22.groupby("Produtos")["Total_em_Dólar"].sum()

    # Crio um DF com os valores acima
    produtosunicos22 = pd.DataFrame({"Quantidade_de_Exportacoes" : produtos_contagem22,
                                     "Valor_total_das_Exportacoes" : produtos_soma22})

    # Incluo o 'Index no meu DF'
    produtosunicos22['ProdutosUnicos22_id'] = produtosunicos22.index

    # Faço uma pequena alteração na saída 
    produtosunicos22["Valor_total_das_Exportacoes"] = produtosunicos22["Valor_total_das_Exportacoes"].map("${:,.0f}".format)

    # Reordeno as 'Colunas' do meu DF
    produtosunicos22 = produtosunicos22[['ProdutosUnicos22_id',
                                         'Quantidade_de_Exportacoes','Valor_total_das_Exportacoes']]

    ##########################################################################
    # Busco localizar os Meses com maiores 'picos' de vendas para o Exterior #
    ##########################################################################

    # Agrupo os Meses e realizo a sua Contagem, em seguida, Somo os Valores de todas as suas Compras
    meses_contagem22 = dadosexp22.groupby("Mes")["Total_em_Dólar"].count()
    meses_soma22 = dadosexp22.groupby("Mes")["Total_em_Dólar"].sum()

    # Crio um DF com os valores acima
    principal_mes22 = pd.DataFrame({"Quantidade_de_Exportacoes_no_Mes" : meses_contagem22,
                                    "Valor_total_das_Exportacoes_no_Mes" : meses_soma22})

    # Incluo o 'Index no meu DF'
    principal_mes22['principal_mes22_id'] = principal_mes22.index

    # Faço uma pequena alteração na saída 
    principal_mes22["Valor_total_das_Exportacoes_no_Mes"] = principal_mes22["Valor_total_das_Exportacoes_no_Mes"].map("${:,.0f}".format)

    # Reordeno as 'Colunas' do meu DF
    principal_mes22 = principal_mes22[['principal_mes22_id',
                                       'Quantidade_de_Exportacoes_no_Mes','Valor_total_das_Exportacoes_no_Mes']]




    return {'exportacoes22':exportacoes22.to_dict(orient='dict'),
            'uf_analises22':uf_analises22.to_dict(orient='dict'),
            'mun_analises22':mun_analises22.to_dict(orient='dict'),
            'porcentagem_dest22':porcentagem_dest22.to_dict(orient='dict'),
            'produtosunicos22':produtosunicos22.to_dict(orient='dict'),
            'principal_mes22':principal_mes22.to_dict(orient='dict')}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

