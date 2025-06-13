from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import io
import base64
import openpyxl
import re
import traceback

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        response = {'status': 'OK', 'message': 'API funcionando!'}
        self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            content_type = self.headers.get('Content-Type', '')
            boundary = content_type.split('boundary=')[1]
            
            files, form_data = self.parse_multipart(post_data, boundary)
            
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            incluir_creditos = form_data.get('incluir_creditos', 'false') == 'true'
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos necessários não foram enviados")
            
            # Processar Excel
            categorias = self.processar_excel(excel_data)
            
            # Processar CSV
            df = self.processar_csv(csv_data, incluir_creditos)
            
            # Categorizar
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar(x, categorias))
            
            # Agrupar resultados
            resultados = df.groupby('Categoria').agg({
                'Valor': ['sum', 'count']
            }).reset_index()
            
            resultados.columns = ['categoria', 'total', 'quantidade']
            valor_total = df['Valor'].sum()
            
            if valor_total > 0:
                resultados['percentual'] = (resultados['total'] / valor_total) * 100
            else:
                resultados['percentual'] = 0
            
            resultados = resultados.sort_values('total', ascending=False)
            
            # Preparar resposta
            categorias_detalhadas = []
            for _, row in resultados.iterrows():
                categoria = row['categoria']
                itens_cat = df[df['Categoria'] == categoria]
                
                itens = []
                for _, item in itens_cat.iterrows():
                    itens.append({
                        'data': item['Data'],
                        'descricao': item['Descricao'],
                        'valor': float(item['Valor']),
                        'tipo': item['Tipo'],
                        'documento': str(item.get('Documento', ''))
                    })
                
                categorias_detalhadas.append({
                    'categoria': categoria,
                    'total': float(row['total']),
                    'quantidade': int(row['quantidade']),
                    'percentual': float(row['percentual']),
                    'itens': itens
                })
            
            # Gerar Excel
            excel_b64 = self.gerar_excel(categorias_detalhadas, df)
            
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_transacoes': len(df),
                    'total_debitos': len(df[df['Tipo'] == 'D']),
                    'total_creditos': len(df[df['Tipo'] == 'C']),
                    'valor_total': float(valor_total)
                },
                'categorias': categorias_detalhadas,
                'excel_file': excel_b64
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(resposta).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {'success': False, 'error': str(e)}
            self.wfile.write(json.dumps(error_response).encode())
    
    def parse_multipart(self, body, boundary):
        parts = body.split(f'--{boundary}'.encode())
        files = {}
        form_data = {}
        
        for part in parts:
            if b'Content-Disposition' not in part:
                continue
            
            header_end = part.find(b'\r\n\r\n')
            if header_end == -1:
                continue
            
            header = part[:header_end].decode('utf-8', errors='ignore')
            content = part[header_end + 4:].rstrip(b'\r\n-')
            
            if 'name="' in header:
                name_start = header.find('name="') + 6
                name_end = header.find('"', name_start)
                name = header[name_start:name_end]
                
                if 'filename="' in header:
                    files[name] = content
                else:
                    form_data[name] = content.decode('utf-8', errors='ignore')
        
        return files, form_data
    
    def processar_excel(self, excel_data):
        try:
            df = pd.read_excel(io.BytesIO(excel_data))
            
            if len(df.columns) < 2:
                raise Exception("Excel deve ter pelo menos 2 colunas")
            
            df.columns = ['Grupo', 'Palavra_Chave'] + list(df.columns[2:])
            
            categorias = {}
            categoria_atual = None
            
            for _, row in df.iterrows():
                if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                    categoria_atual = str(row['Grupo']).strip()
                
                if pd.notna(row['Palavra_Chave']) and categoria_atual:
                    palavra = str(row['Palavra_Chave']).strip()
                    categorias[palavra] = categoria_atual
            
            return categorias
        except Exception as e:
            raise Exception(f"Erro no Excel: {e}")
    
    def processar_csv(self, csv_data, incluir_creditos):
        try:
            # Tentar diferentes codificações
            csv_string = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    break
                except:
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível ler o CSV")
            
            # Limpar caracteres
            csv_string = csv_string.replace('Histórico', 'Historico')
            csv_string = csv_string.replace('Número', 'Numero')
            
            df = pd.read_csv(io.StringIO(csv_string))
            
            # Detectar formato
            if 'Descrição' in df.columns or 'Descricao' in df.columns:
                # Formato antigo
                desc_col = 'Descrição' if 'Descrição' in df.columns else 'Descricao'
                df = df.dropna(subset=[desc_col, 'Valor'])
                df['Descricao'] = df[desc_col]
                df['Agencia'] = df.get('Agência', df.get('Agencia', ''))
                df['Documento'] = df.get('Documento', '')
                
                if not incluir_creditos:
                    df = df[df['Tipo'] == 'D']
                    
            elif 'Historico' in df.columns:
                # Formato novo
                df = df.dropna(subset=['Historico', 'Valor'])
                df = df[df['Historico'] != 'Saldo Anterior']
                
                df['Descricao'] = df['Historico']
                df['Agencia'] = df.get('Dependencia Origem', '')
                df['Documento'] = df.get('Numero do documento', '')
                df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
                df['Valor'] = df['Valor'].abs()
                
                if not incluir_creditos:
                    df = df[df['Tipo'] == 'D']
            else:
                raise Exception("Formato de CSV não reconhecido")
            
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(subset=['Valor'])
            
            return df
        except Exception as e:
            raise Exception(f"Erro no CSV: {e}")
    
    def categorizar(self, descricao, categorias):
        if not descricao or pd.isna(descricao):
            return "Outros"
        
        desc_upper = str(descricao).upper()
        
        # Ordenar por tamanho
        sorted_keys = sorted(categorias.keys(), key=len, reverse=True)
        
        for keyword in sorted_keys:
            if keyword.upper() in desc_upper:
                return categorias[keyword]
        
        return "Outros"
    
    def gerar_excel(self, resultados, df):
        try:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Resumo"
            
            # Cabeçalho
            ws.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
            ws.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws.append([])
            
            # Estatísticas
            ws.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for resultado in resultados:
                ws.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # Salvar
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
        except:
            return None