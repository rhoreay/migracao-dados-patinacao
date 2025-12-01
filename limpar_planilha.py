import pandas as pd

def limpar_dados_csv(arquivo_entrada, arquivo_saida):
    # Carregar a planilha
    df = pd.read_csv(arquivo_entrada)

    # 1. Remover registros CANCELADOS
    # Mantém apenas o que for diferente de 'CANCELADO'
    df = df[df['Status'] != 'CANCELADO'].copy()

    # 2. Sanitizar CPF
    # Converte para string -> Remove tudo que não for número (regex \D) -> Preenche com zeros à esquerda até 11 dígitos
    df['CPF'] = df['CPF'].astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)

    # 3. Mapeamento de Tipo de Público
    # Publico Geral -> geral
    # PCD -> pcd
    mapeamento = {
        'Público Geral': 'geral', 
        'PCD': 'pcd'
    }
    df['TipoVaga'] = df['TipoVaga'].replace(mapeamento)

    # Salvar o novo CSV
    df.to_csv(arquivo_saida, index=False)
    print(f"Arquivo '{arquivo_saida}' gerado com sucesso!")
    print(f"Total de registros processados: {len(df)}")
    print(df[['CPF', 'TipoVaga']].head())

# Execução
if __name__ == "__main__":
    limpar_dados_csv('PLANILHA ORIGINAL.csv', 'resultado_limpeza/inscricoes_sanitizado.csv')