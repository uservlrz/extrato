from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import numpy as np
from datetime import datetime
import io
import base64
import openpyxl
import re
import traceback

def log_error(error_msg):
    """Log de erro para debug"""
    print(f"ERRO: {error_msg}")
    traceback.print_exc()

def categorizar_item(descricao, categorias_dict):
    """Categoriza um item baseado na descrição"""
    try:
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
    except Exception as e:
        log_error(f"Erro ao categorizar '{descricao}': {e}")
        return "Outros"

def processar_categorias_excel(excel_data):
    """Processa Excel de categorias"""
    try:
        df = pd.read_excel(io.BytesIO(excel_data), header=0)
        
        # Verificar se tem pelo menos 2 colunas
        if len(df.columns) < 2:
            raise Exception("Excel deve ter pelo menos 2 colunas")
        
        # Renomear colunas para padrão
        df.columns = ['Grupo', 'Palavra_Chave'] + list(df.columns[2:])
        
        categorias_dict = {}
        categoria_atual = None
        
        for index, row in df.iterrows():
            try:
                if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                    categoria_atual = str(row['Grupo']).strip()
                
                if pd.notna(row['Palavra_Chave']) and str(row['Palavra_Chave']).strip() and categoria_atual:
                    palavra_chave = str(row['Palavra_Chave']).strip()
                    categorias_dict[palavra_chave] = categoria_atual
            except Exception as e:
                log_error(f"Erro na linha {index}: {e}")
                continue
        
        if not categorias_dict:
            raise Exception("Nenhuma categoria válida encontrada no Excel")
        
        return categorias_dict
    except Exception as e:
        log_error(f"Erro ao processar Excel: {e}")
        raise Exception(f"Erro ao processar Excel: {str(e)}")

def processar_extrato_csv(csv_data, incluir_creditos=False):
    """Processa CSV do extrato"""
    try:
        # Tentar diferentes codificações
        csv_string = None
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                csv_string = csv_data.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        
        if csv_string is None:
            raise Exception("Não foi possível decodificar o arquivo CSV")
        
        # Limpar caracteres problemáticos
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        csv_string = csv_string.replace('ó', 'o').replace('ú', 'u').replace('ã', 'a')
        
        df = pd.read_csv(io.StringIO(csv_string))
        
        print(f"Colunas encontradas: {list(df.columns)}")
        print(f"Total de linhas: {len(df)}")
        
        # Detectar formato automaticamente
        if 'Descrição' in df.columns or 'Descricao' in df.columns:
            # Formato antigo
            desc_col = 'Descrição' if 'Descrição' in df.columns else 'Descricao'
            df = df.dropna(subset=[desc_col, 'Valor'])
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(subset=['Valor'])
            
            # Padronizar nomes
            df['Descrição'] = df[desc_col]
            
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']
                
        elif 'Historico' in df.columns or 'Histórico' in df.columns:
            # Formato novo
            hist_col = 'Historico' if 'Historico' in df.columns else 'Histórico'
            df = df.dropna(subset=[hist_col, 'Valor'])
            df = df[df[hist_col] != 'Saldo Anterior']
            
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(subset=['Valor'])
            
            # Converter para formato padrão
            df['Descrição'] = df[hist_col]
            df['Agência'] = df.get('Dependencia Origem', '')
            df['Documento'] = df.get('Numero do documento', df.get('Número do documento', ''))
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
            
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']
        else:
            raise Exception(f"Formato de CSV não reconhecido. Colunas: {list(df.columns)}")
        
        # Converter datas
        try:
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        except:
            pass
        
        print(f"Linhas processadas: {len(df)}")
        return df
        
    except Exception as e:
        log_error(f"Erro ao processar CSV: {e}")
        raise Exception(f"Erro ao processar CSV: {str(e)}")

def parse_multipart_simple(body, boundary):
    """Parse simples e robusto de multipart form data"""
    try:
        parts = body.split(f'--{boundary}'.encode())
        files = {}
        form_data = {}
        
        for part in parts:
            if b'Content-Disposition' not in part:
                continue
            
            try:
                # Encontrar o cabeçalho
                header_end = part.find(b'\r\n\r\n')
                if header_end == -1:
                    continue
                
                header = part[:header_end].decode('utf-8', errors='ignore')
                content = part[header_end + 4:]
                
                # Remover terminadores
                if content.endswith(b'\r\n'):
                    content = content[:-2]
                
                # Extrair nome do campo
                if 'name="' in header:
                    name_start = header.find('name="') + 6
                    name_end = header.find('"', name_start)
                    name = header[name_start:name_end]
                    
                    if 'filename="' in header:
                        # É um arquivo
                        files[name] = content
                    else:
                        # É um campo de texto
                        form_data[name] = content.decode('utf-8', errors='ignore')
            except Exception as e:
                log_error(f"Erro ao processar parte do multipart: {e}")
                continue
        
        return files, form_data
    except Exception as e:
        log_error(f"Erro no parse multipart: {e}")
        return {}, {}

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
        try:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            response = {
                'status': 'OK',
                'message': 'API Python funcionando no Vercel!',
                'timestamp': datetime.now().isoformat(),
                'pandas_version': pd.__version__
            }
            self.wfile.write(json.dumps(response).encode())
        except Exception as e:
            log_error(f"Erro no GET: {e}")
            self.send_error(500, str(e))
    
    def do_POST(self):
        """Processar upload de arquivos"""
        try:
            print("Iniciando processamento POST...")
            
            # Ler dados do POST
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                raise Exception("Nenhum dados recebido")
            
            post_data = self.rfile.read(content_length)
            print(f"Dados recebidos: {len(post_data)} bytes")
            
            # Parse multipart form data
            content_type = self.headers.get('Content-Type', '')
            if 'multipart/form-data' not in content_type:
                raise Exception("Content-Type deve ser multipart/form-data")
            
            # Extrair boundary
            boundary = content_type.split('boundary=')[1]
            print(f"Boundary: {boundary}")
            
            # Parse arquivos e dados
            files, form_data = parse_multipart_simple(post_data, boundary)
            print(f"Arquivos encontrados: {list(files.keys())}")
            print(f"Form data: {list(form_data.keys())}")
            
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            incluir_creditos = form_data.get('incluir_creditos', 'false').lower() == 'true'
            
            if not csv_data:
                raise Exception("Arquivo CSV não encontrado")
            if not excel_data:
                raise Exception("Arquivo Excel não encontrado")
            
            print(f"CSV: {len(csv_data)} bytes, Excel: {len(excel_data)} bytes")
            
            # Processar arquivos
            print("Processando Excel...")
            categorias_dict = processar_categorias_excel(excel_data)
            print(f"Categorias carregadas: {len(categorias_dict)}")
            
            print("Processando CSV...")
            df_extrato = processar_extrato_csv(csv_data, incluir_creditos)
            print(f"Transações processadas: {len(df_extrato)}")
            
            # Categorizar itens
            print("Categorizando...")
            df_extrato['Categoria'] = df_extrato['Descrição'].apply(
                lambda x: categorizar_item(x, categorias_dict)
            )
            
            # Calcular totais por categoria
            print("Calculando totais...")
            resultados_categoria = df_extrato.groupby('Categoria').agg({
                'Valor': ['sum', 'count']
            }).reset_index()
            
            resultados_categoria.columns = ['categoria', 'total', 'quantidade']
            valor_total_geral = df_extrato['Valor'].sum()
            
            if valor_total_geral > 0:
                resultados_categoria['percentual'] = (resultados_categoria['total'] / valor_total_geral) * 100
            else:
                resultados_categoria['percentual'] = 0
                
            resultados_categoria = resultados_categoria.sort_values('total', ascending=False)
            
            # Preparar dados detalhados
            print("Preparando resultados...")
            resultados_detalhados = []
            
            for _, categoria_row in resultados_categoria.iterrows():
                categoria = categoria_row['categoria']
                itens_categoria = df_extrato[df_extrato['Categoria'] == categoria].copy()
                itens_categoria = itens_categoria.sort_values('Valor', ascending=False)
                
                itens_lista = []
                for _, item in itens_categoria.iterrows():
                    itens_lista.append({
                        'data': item['Data'].isoformat() if pd.notna(item['Data']) else None,
                        'descricao': str(item['Descrição']),
                        'valor': float(item['Valor']),
                        'tipo': str(item['Tipo']),
                        'documento': str(item.get('Documento', ''))
                    })
                
                resultados_detalhados.append({
                    'categoria': str(categoria),
                    'total': float(categoria_row['total']),
                    'quantidade': int(categoria_row['quantidade']),
                    'percentual': float(categoria_row['percentual']),
                    'itens': itens_lista
                })
            
            # Resposta (sem Excel por enquanto para debug)
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_transacoes': len(df_extrato),
                    'total_debitos': len(df_extrato[df_extrato['Tipo'] == 'D']),
                    'total_creditos': len(df_extrato[df_extrato['Tipo'] == 'C']),
                    'valor_total': float(valor_total_geral)
                },
                'categorias': resultados_detalhados,
                'excel_file': None  # Remover Excel por enquanto
            }
            
            print("Enviando resposta...")
            
            # Enviar resposta
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            self.wfile.write(json.dumps(resposta).encode())
            print("Processamento concluído com sucesso!")
            
        except Exception as e:
            log_error(f"Erro no POST: {e}")
            
            # Enviar erro detalhado
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {
                'success': False, 
                'error': str(e),
                'traceback': traceback.format_exc()
            }
            self.wfile.write(json.dumps(error_response).encode())