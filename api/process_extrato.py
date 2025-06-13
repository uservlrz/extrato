from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import numpy as np
from datetime import datetime
import io
import base64
import openpyxl
import re
import urllib.parse

def categorizar_item(descricao, categorias_dict):
    """Categoriza um item baseado na descrição"""
    if not descricao or pd.isna(descricao):
        return "Outros"
    
    descricao_upper = str(descricao).upper()
    
    # Ordenar palavras-chave por tamanho (maiores primeiro)
    sorted_keywords = sorted(categorias_dict.keys(), key=len, reverse=True)
    
    for keyword in sorted_keywords:
        keyword_upper = keyword.upper()
        
        # Para palavras curtas, verificar palavra completa
        if len(keyword_upper) <= 4:
            pattern = r'\b' + re.escape(keyword_upper) + r'\b'
            if re.search(pattern, descricao_upper):
                return categorias_dict[keyword]
        else:
            # Para palavras maiores, busca substring
            if keyword_upper in descricao_upper:
                return categorias_dict[keyword]
    
    return "Outros"

def processar_categorias_excel(excel_data):
    """Processa Excel de categorias"""
    try:
        df = pd.read_excel(io.BytesIO(excel_data), header=0)
        df.columns = ['Grupo', 'Palavra_Chave']
        
        categorias_dict = {}
        categoria_atual = None
        
        for index, row in df.iterrows():
            if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                categoria_atual = str(row['Grupo']).strip()
            
            if pd.notna(row['Palavra_Chave']) and str(row['Palavra_Chave']).strip() and categoria_atual:
                palavra_chave = str(row['Palavra_Chave']).strip()
                categorias_dict[palavra_chave] = categoria_atual
        
        return categorias_dict
    except Exception as e:
        raise Exception(f"Erro ao processar Excel: {str(e)}")

def processar_extrato_csv(csv_data, incluir_creditos=False):
    """Processa CSV do extrato"""
    try:
        # Tentar UTF-8 primeiro, depois Latin-1
        try:
            csv_string = csv_data.decode('utf-8')
        except UnicodeDecodeError:
            csv_string = csv_data.decode('latin1')
        
        # Limpar caracteres problemáticos
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        
        df = pd.read_csv(io.StringIO(csv_string))
        
        # Detectar formato automaticamente
        if 'Descrição' in df.columns:
            # Formato antigo
            df = df.dropna(subset=['Descrição', 'Valor'])
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(subset=['Valor'])
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']
                
        elif 'Historico' in df.columns or 'Histórico' in df.columns:
            # Formato novo
            hist_col = 'Historico' if 'Historico' in df.columns else 'Histórico'
            df = df.dropna(subset=[hist_col, 'Valor'])
            df = df[df[hist_col] != 'Saldo Anterior']
            
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(subset=['Valor'])
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            
            # Converter para formato padrão
            df['Descrição'] = df[hist_col]
            df['Agência'] = df.get('Dependencia Origem', '')
            df['Documento'] = df.get('Numero do documento', df.get('Número do documento', ''))
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
            
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']
        else:
            raise Exception("Formato de CSV não reconhecido")
        
        return df
    except Exception as e:
        raise Exception(f"Erro ao processar CSV: {str(e)}")

def gerar_excel_completo(resultados, dados_processados):
    """Gera Excel completo"""
    try:
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        
        # Aba Resumo
        ws_resumo = wb.create_sheet("Resumo")
        ws_resumo.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
        ws_resumo.append([f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"])
        ws_resumo.append([])
        
        # Estatísticas
        total_transacoes = len(dados_processados)
        total_debitos = len(dados_processados[dados_processados['Tipo'] == 'D'])
        total_creditos = len(dados_processados[dados_processados['Tipo'] == 'C'])
        valor_total = dados_processados['Valor'].sum()
        
        ws_resumo.append(["ESTATÍSTICAS GERAIS"])
        ws_resumo.append(["Total de Transações", total_transacoes])
        ws_resumo.append(["Total de Débitos", total_debitos])
        ws_resumo.append(["Total de Créditos", total_creditos])
        ws_resumo.append(["Valor Total", f"R$ {valor_total:,.2f}"])
        ws_resumo.append([])
        
        # Resumo por categoria
        ws_resumo.append(["RESUMO POR CATEGORIA"])
        ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
        
        for resultado in resultados:
            valor_formatado = f"R$ {resultado['total']:,.2f}"
            ws_resumo.append([
                resultado['categoria'],
                valor_formatado,
                resultado['quantidade'],
                f"{resultado['percentual']:.1f}%"
            ])
        
        # Salvar em buffer
        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)
        
        return base64.b64encode(excel_buffer.getvalue()).decode()
    except Exception as e:
        raise Exception(f"Erro ao gerar Excel: {str(e)}")

def parse_multipart(body, boundary):
    """Parse simples de multipart form data"""
    parts = body.split(f'--{boundary}'.encode())
    files = {}
    form_data = {}
    
    for part in parts:
        if b'Content-Disposition' not in part:
            continue
            
        lines = part.split(b'\r\n')
        headers = []
        content_start = 0
        
        for i, line in enumerate(lines):
            if line == b'':
                content_start = i + 1
                break
            headers.append(line.decode('utf-8', errors='ignore'))
        
        content = b'\r\n'.join(lines[content_start:]).rstrip(b'\r\n-')
        
        # Extrair nome do campo
        for header in headers:
            if 'name=' in header:
                if 'filename=' in header:
                    # É um arquivo
                    name = header.split('name="')[1].split('"')[0]
                    files[name] = content
                else:
                    # É um campo de texto
                    name = header.split('name="')[1].split('"')[0]
                    form_data[name] = content.decode('utf-8', errors='ignore')
                break
    
    return files, form_data

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        """Health check"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        response = {
            'status': 'OK',
            'message': 'API Python funcionando no Vercel!',
            'timestamp': datetime.now().isoformat()
        }
        self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        """Processar upload de arquivos"""
        try:
            # Ler dados do POST
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            # Parse multipart form data
            content_type = self.headers.get('Content-Type', '')
            if 'multipart/form-data' not in content_type:
                raise Exception("Content-Type deve ser multipart/form-data")
            
            # Extrair boundary
            boundary = content_type.split('boundary=')[1]
            
            # Parse arquivos e dados
            files, form_data = parse_multipart(post_data, boundary)
            
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            incluir_creditos = form_data.get('incluir_creditos', 'false').lower() == 'true'
            
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
            resultados_categoria = resultados_categoria.sort_values('total', ascending=False)
            
            # Preparar dados detalhados
            resultados_detalhados = []
            
            for _, categoria_row in resultados_categoria.iterrows():
                categoria = categoria_row['categoria']
                itens_categoria = df_extrato[df_extrato['Categoria'] == categoria].copy()
                itens_categoria = itens_categoria.sort_values('Valor', ascending=False)
                
                itens_lista = []
                for _, item in itens_categoria.iterrows():
                    itens_lista.append({
                        'data': item['Data'].isoformat() if pd.notna(item['Data']) else None,
                        'descricao': item['Descrição'],
                        'valor': float(item['Valor']),
                        'tipo': item['Tipo'],
                        'documento': str(item.get('Documento', ''))
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
            
            # Resposta
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
            
            self.wfile.write(json.dumps(resposta).encode())
            
        except Exception as e:
            # Erro
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {'success': False, 'error': str(e)}
            self.wfile.write(json.dumps(error_response).encode())