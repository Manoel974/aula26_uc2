import polars as pl 
from datetime import datetime
# import os 
import gc
import numpy as np 


try: 
    ENDERECO_DADOS = r'./dados/'

    inicio = datetime.now()

    hora_import = datetime.now()
    
    print('Iniciando leitura do arquivo..')

    lista_arquivos = ['202405_NovoBolsaFamilia.csv', '202404_NovoBolsaFamilia.csv']

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df


        del df

        gc.collect() 

    # convertendo o valor da parcela para float
    df_bolsa_familia = df_bolsa_familia.with_columns(
    pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64))

    print(df_bolsa_familia)
    
    # criando o arquivo parquet
    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')    
    
    del df_bolsa_familia

    hora_impressao = datetime.now()

    print(f'Tempo de execução: {hora_impressao - hora_import}')
    fim = datetime.now()

    print(f'Tempo de execução: {fim - inicio}')
    print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print('Erro ao obter dados: ')

