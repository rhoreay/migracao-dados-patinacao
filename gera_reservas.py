import pandas as pd
from datetime import datetime, timedelta

def processar_migracao(arquivo_entrada, arquivo_saida):
    df = pd.read_csv(arquivo_entrada)

    DATA_INICIO = datetime(2025, 11, 29)
    HORA_INICIO_DIA = datetime.strptime("17:00", "%H:%M")
    
    # ordem do tamanho dos patins
    TAMANHOS_ORDEM = [
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 
        36, 37, 38, 39, 40, 41, 42, 43, 44
    ]
    QTD_TAMANHOS = len(TAMANHOS_ORDEM) 
    SESSOES_POR_DIA = 23 
    
    def get_sessao_id(row):
        try:
            # converte string para Date Object
            data_evento = datetime.strptime(row['Data'], "%d/%m/%Y")
            hora_evento = datetime.strptime(row['Horario'], "%H:%M")

            # algoritmo para "adivinhar" os ids, partindo do pnoto que os ids comecam no 0. (tabela trucada)
            
            # 1. Calcular quantos dias se passaram desde o início
            delta_dias = (data_evento - DATA_INICIO).days
            
            if delta_dias < 0:
                return None

            # 2. Calcular qual o índice da sessão dentro do dia (0 a 22)
            # Diferença em minutos desde as 17:00
            diff_minutos = (hora_evento - HORA_INICIO_DIA).seconds // 60
            slot_index = diff_minutos // 15
            
            # ID Base = 1
            # Pulo dias = dias * 23
            # Pulo horas = slot_index
            sessao_id = 1 + (delta_dias * SESSOES_POR_DIA) + slot_index
            return int(sessao_id)
        except Exception as e:
            return None

    def get_estoque_id(row):
        sessao_id = row['sessao_id']
        tamanho = row['NumeroCalcado']
        
        if pd.isna(sessao_id) or tamanho not in TAMANHOS_ORDEM:
            return None
            
        # Índice do tamanho (0 a 18)
        tamanho_index = TAMANHOS_ORDEM.index(tamanho)
        
        # ID Base = 1
        # Pulo sessões anteriores = (sessao_id - 1) * 19 registros de estoque por sessão
        estoque_id = 1 + ((sessao_id - 1) * QTD_TAMANHOS) + tamanho_index
        return int(estoque_id)
    
    print("Calculando IDs de Sessão...")
    df['sessao_id'] = df.apply(get_sessao_id, axis=1)
    
    print("Calculando IDs de Estoque...")
    df['estoque_id'] = df.apply(get_estoque_id, axis=1)

    
    # data frame final 
    
    df_final = pd.DataFrame()
    
    # Mapeamento
    df_final['codigo_reserva'] = df['CodigoIngresso']
    df_final['sessao_id'] = df['sessao_id']
    df_final['estoque_id'] = df['estoque_id']
    df_final['cpf'] = df['CPF'] # Já sanitizado no passo anterior
    df_final['nome_completo'] = df['Nome']
    df_final['email'] = df['Email']
    df_final['telefone'] = df['Telefone']
    df_final['cep'] = df['CEP']
    df_final['cidade'] = df['Cidade']
    df_final['bairro'] = df['Bairro']
    df_final['tipo_publico'] = df['TipoVaga'] # Já sanitizado (geral/pcd)
    
    # Formatar Timestamp para MySQL (YYYY-MM-DD HH:MM:SS)
    # O input está "11/24/2025 6:51:57" (MM/DD/YYYY H:M:S) ou similar
    # Ajuste o formato de entrada conforme seu CSV real se der erro
    df_final['data_criacao'] = pd.to_datetime(df['Timestamp'], format='mixed', dayfirst=False).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df_final['situacao'] = 'confirmado' # Ou 'CONFIRMADO', conforme seu ENUM/Varchar

    # Remover linhas onde não foi possível calcular ID (ex: datas fora do range)
    df_final = df_final.dropna(subset=['sessao_id', 'estoque_id'])
    
    # Converter IDs para int (pandas tende a virar float se houver NaNs antes)
    df_final['sessao_id'] = df_final['sessao_id'].astype(int)
    df_final['estoque_id'] = df_final['estoque_id'].astype(int)

    print(f"Gerando CSV final com {len(df_final)} registros...")
    df_final.to_csv(arquivo_saida, index=False)
    print("Concluído.")

# Execução
if __name__ == "__main__":
    # Usa o arquivo sanitizado do passo anterior
    processar_migracao('resultado_limpeza/inscricoes_sanitizado.csv', 'resultado_final/importacao_reservas_final.csv')