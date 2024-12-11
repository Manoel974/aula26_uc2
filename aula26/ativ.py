import polars as pl 
from datetime import datetime
import os 
import gc
import numpy as np 
from matplotlib import pyplot as plt 



ENDERECO_DADOS = r'./dados/'

try:

    print('Iniciando leitura do arquivo parquet....')

    inicio = datetime.now()
    
    df_bolsa_familia_plan = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia = df_bolsa_familia_plan.collect()

    print(df_bolsa_familia)

    fim = datetime.now()

    print(f'Tempo de execução para leitura de parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso!!')
 

    # convertendo o valor da parcela para float
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
    )
    # criando o arquivo parquet
    # df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')    


    # fim = datetime.now()
    
    print(f'Tempo de execução: {fim - inicio}')
    # print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print('Erro ao obter dados: ')

