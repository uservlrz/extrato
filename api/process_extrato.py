import json
import pandas as pd
import numpy as np
from datetime import datetime
import io
import base64
from http.server import BaseHTTPRequestHandler
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
import re
import urllib.parse

def categorizar_item(descricao, categorias_dict):
    """
    Categoriza um item baseado na descrição usando as palavras-chave
    """
    if not descricao or pd.isna(descricao):
        return "Outros"
    
    descricao_upper = str(descricao).upper()
    
    # Ordenar palavras-chave por tamanho (maiores primeiro) para matches mais específicos
    sorted_keywords = sorted(categorias_dict.keys(), key=len, reverse=True)
    
    for keyword in sorted_keywords:
        keyword_upper = keyword.upper()
        
        # Para palavras curtas (<=4 chars), verificar palavra completa
        if len(keyword_upper) <= 4:
            # Usar regex para palavra completa
            pattern = r'\b' + re.escape(keyword_upper) + r'\b'
            if re.search(pattern, descricao_upper):
                return categorias_dict[keyword]
        else:
            # Para palavras maiores, busca substring
            if keyword_upper in descricao_upper:
                return categorias_dict[keyword]
    
    return "Outros"

def processar_categorias_excel(excel_data):
    """
    Processa o arquivo Excel de categorias e retorna dicionário
    """
    try:
        # Ler Excel
        df = pd.read_excel(io.BytesIO(excel_data), header=0)
        
        # Renomear colunas para facilitar
        df.columns = ['Grupo', 'Palavra_Chave']
        
        categorias_dict = {}
        categoria_atual = None
        
        for index, row in df.iterrows():
            # Se há um grupo definido
            if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                categoria_atual = str(row['Grupo']).strip()
            
            # Se há uma palavra-chave
            if pd.notna(row['Palavra_Chave']) and str(row['Palavra_Chave']).strip() and categoria_atual:
                palavra_chave = str(row['Palavra_Chave']).strip()
                categorias_dict[palavra_chave] = categoria_atual
        
        return categorias_dict
    
    except Exception as e:
        raise Exception(f"Erro ao processar Excel de categorias: {str(e)}")

def processar_extrato_csv(csv_data, incluir_creditos=False):
    """
    Processa o arquivo CSV do extrato
    """
    try:
        # Ler CSV com configurações robustas
        df = pd.read_csv(io.BytesIO(csv_data), encoding='utf-8')
        
        # Verificar colunas obrigatórias
        colunas_necessarias = ['Data', 'Descrição', 'Valor', 'Tipo']
        for coluna in colunas_necessarias:
            if coluna not in df.columns:
                raise Exception(f"Coluna '{coluna}' não encontrada no CSV")
        
        # Limpar dados
        df = df.dropna(subset=['Descrição', 'Valor'])
        
        # Converter valor para numérico
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df = df.dropna(subset=['Valor'])
        
        # Converter data para datetime
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        
        # Filtrar por tipo se necessário
        if not incluir_creditos:
            df = df[df['Tipo'] == 'D']
        
        return df
    
    except Exception as e:
        raise Exception(f"Erro ao processar CSV: {str(e)}")

def gerar_excel_completo(resultados, dados_processados):
    """
    Gera arquivo Excel completo com múltiplas abas
    """
    try:
        # Criar workbook
        wb = openpyxl.Workbook()
        
        # Remover aba padrão
        wb.remove(wb.active)
        
        # 1. Aba Resumo
        ws_resumo = wb.create_sheet("Resumo")
        
        # Cabeçalho
        ws_resumo.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
        ws_resumo.append([f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
        ws_resumo.append([])
        
        # Estatísticas gerais
        total_transacoes = len(dados_processados)
        total_debitos = len(dados_processados[dados_processados['Tipo'] == 'D'])
        total_creditos = len(dados_processados[dados_processados['Tipo'] == 'C'])
        valor_total = dados_processados['Valor'].sum()
        
        ws_resumo.append(["ESTATÍSTICAS GERAIS"])
        ws_resumo.append(["Total de Transações", total_transacoes])
        ws_resumo.append(["Total de Débitos", total_debitos])
        ws_resumo.append(["Total de Créditos", total_creditos])
        ws_resumo.append(["Valor Total", f"R$ {valor_total:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')])
        ws_resumo.append([])
        
        # Resumo por categoria
        ws_resumo.append(["RESUMO POR CATEGORIA"])
        ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
        
        for resultado in resultados:
            valor_formatado = f"R$ {resultado['total']:.2f}".replace('.', ',')
            if resultado['total'] >= 1000:
                valor_formatado = f"R$ {resultado['total']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            
            ws_resumo.append([
                resultado['categoria'],
                valor_formatado,
                resultado['quantidade'],
                f"{resultado['percentual']:.1f}%"
            ])
        
        # 2. Aba para cada categoria
        for resultado in resultados:
            categoria = resultado['categoria']
            # Limitar nome da aba e remover caracteres inválidos
            nome_aba = re.sub(r'[\\/*?:"<>|]', '', categoria)[:31]
            
            ws_categoria = wb.create_sheet(nome_aba)
            
            # Cabeçalho da categoria
            ws_categoria.append([f"CATEGORIA: {categoria}"])
            ws_categoria.append([f"Total: R$ {resultado['total']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')])
            ws_categoria.append([f"Quantidade: {resultado['quantidade']} itens"])
            ws_categoria.append([f"Percentual: {resultado['percentual']:.1f}%"])
            ws_categoria.append([])
            
            # Cabeçalho da tabela
            ws_categoria.append(["#", "Data", "Descrição", "Valor", "Tipo"])
            
            # Itens da categoria
            for i, item in enumerate(resultado['itens'], 1):
                data_formatada = item['data'].strftime('%d/%m/%Y') if pd.notna(item['data']) else 'Sem data'
                valor_formatado = f"R$ {item['valor']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                tipo_formatado = "CRÉDITO" if item['tipo'] == 'C' else "DÉBITO"
                
                ws_categoria.append([
                    i,
                    data_formatada,
                    item['descricao'],
                    valor_formatado,
                    tipo_formatado
                ])
            
            # Total da categoria
            ws_categoria.append([])
            total_formatado = f"R$ {resultado['total']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            ws_categoria.append(["", "", "TOTAL DA CATEGORIA:", total_formatado, ""])
        
        # Salvar em buffer
        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)
        
        # Converter para base64 para envio
        excel_b64 = base64.b64encode(excel_buffer.getvalue()).decode()
        
        return excel_b64
    
    except Exception as e:
        raise Exception(f"Erro ao gerar Excel: {str(e)}")

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Verificar se é a rota correta
            if not self.path.startswith('/api/process_extrato'):
                self.send_response(404)
                self.end_headers()
                return
            
            # Ler dados do POST
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            # Parse multipart form data
            content_type = self.headers['Content-Type']
            if 'multipart/form-data' not in content_type:
                raise Exception("Content-Type deve ser multipart/form-data")
            
            # Extrair boundary
            boundary = content_type.split('boundary=')[1].encode()
            
            # Parse dos arquivos
            parts = post_data.split(b'--' + boundary)
            
            csv_data = None
            excel_data = None
            incluir_creditos = False
            
            for part in parts:
                if b'name="csv_file"' in part:
                    # Extrair dados do CSV
                    csv_start = part.find(b'\r\n\r\n') + 4
                    csv_data = part[csv_start:].rstrip(b'\r\n')
                elif b'name="excel_file"' in part:
                    # Extrair dados do Excel
                    excel_start = part.find(b'\r\n\r\n') + 4
                    excel_data = part[excel_start:].rstrip(b'\r\n')
                elif b'name="incluir_creditos"' in part:
                    # Extrair valor do checkbox
                    value_start = part.find(b'\r\n\r\n') + 4
                    value = part[value_start:].rstrip(b'\r\n').decode()
                    incluir_creditos = value.lower() == 'true'
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos CSV e Excel são obrigatórios")
            
            # Processar arquivos
            categorias_dict = processar_categorias_excel(excel_data)
            df_extrato = processar_extrato_csv(csv_data, incluir_creditos)
            
            # Categorizar itens
            df_extrato['Categoria'] = df_extrato['Descrição'].apply(
                lambda x: categorizar_item(x, categorias_dict)
            )
            
            # Calcular totais por categoria
            resultados_categoria = df_extrato.groupby('Categoria').agg({
                'Valor': ['sum', 'count']
            }).reset_index()
            
            resultados_categoria.columns = ['categoria', 'total', 'quantidade']
            valor_total_geral = df_extrato['Valor'].sum()
            resultados_categoria['percentual'] = (resultados_categoria['total'] / valor_total_geral) * 100
            
            # Ordenar por valor total (maior primeiro)
            resultados_categoria = resultados_categoria.sort_values('total', ascending=False)
            
            # Preparar dados detalhados
            resultados_detalhados = []
            
            for _, categoria_row in resultados_categoria.iterrows():
                categoria = categoria_row['categoria']
                itens_categoria = df_extrato[df_extrato['Categoria'] == categoria].copy()
                
                # Ordenar itens por valor (maior primeiro)
                itens_categoria = itens_categoria.sort_values('Valor', ascending=False)
                
                itens_lista = []
                for _, item in itens_categoria.iterrows():
                    itens_lista.append({
                        'data': item['Data'],
                        'descricao': item['Descrição'],
                        'valor': float(item['Valor']),
                        'tipo': item['Tipo'],
                        'documento': item.get('Documento', '')
                    })
                
                resultados_detalhados.append({
                    'categoria': categoria,
                    'total': float(categoria_row['total']),
                    'quantidade': int(categoria_row['quantidade']),
                    'percentual': float(categoria_row['percentual']),
                    'itens': itens_lista
                })
            
            # Gerar Excel
            excel_b64 = gerar_excel_completo(resultados_detalhados, df_extrato)
            
            # Preparar resposta
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_transacoes': len(df_extrato),
                    'total_debitos': len(df_extrato[df_extrato['Tipo'] == 'D']),
                    'total_creditos': len(df_extrato[df_extrato['Tipo'] == 'C']),
                    'valor_total': float(valor_total_geral)
                },
                'categorias': resultados_detalhados,
                'excel_file': excel_b64
            }
            
            # Enviar resposta
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            response_json = json.dumps(resposta, default=str)
            self.wfile.write(response_json.encode())
            
        except Exception as e:
            # Enviar erro
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = json.dumps({'error': str(e)})
            self.wfile.write(error_response.encode())
    
    def do_GET(self):
        if self.path == '/api/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            response = json.dumps({'status': 'OK', 'message': 'API funcionando'})
            self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()