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
        
        response = {'status': 'OK', 'message': 'API de Extratos Bancários funcionando!'}
        self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        try:
            print("=== INICIANDO PROCESSAMENTO EXTRATOS ===")
            
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            print(f"Dados recebidos: {len(post_data)} bytes")
            
            content_type = self.headers.get('Content-Type', '')
            if 'boundary=' not in content_type:
                raise Exception("Content-Type inválido - boundary não encontrado")
            
            boundary = content_type.split('boundary=')[1]
            print(f"Boundary: {boundary}")
            
            files, form_data = self.parse_multipart(post_data, boundary)
            print(f"Arquivos encontrados: {list(files.keys())}")
            print(f"Dados do formulário: {list(form_data.keys())}")
            
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos CSV e Excel são necessários")
            
            print(f"CSV: {len(csv_data)} bytes")
            print(f"Excel: {len(excel_data)} bytes")
            
            # Processar Excel
            print("Processando Excel...")
            categorias = self.processar_excel(excel_data)
            print(f"Categorias encontradas: {len(categorias)}")
            
            # Processar CSV
            print("Processando CSV...")
            df = self.processar_csv(csv_data)
            print(f"Linhas processadas: {len(df)}")
            
            # Categorizar
            print("Categorizando transações...")
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar(x, categorias))
            
            # Separar créditos e débitos
            df_creditos = df[df['Tipo'] == 'C'].copy()
            df_debitos = df[df['Tipo'] == 'D'].copy()
            
            print(f"Créditos: {len(df_creditos)} transações")
            print(f"Débitos: {len(df_debitos)} transações")
            
            # Análises
            resultados_gerais = self.analisar_categorias(df, "geral")
            resultados_creditos = self.analisar_categorias(df_creditos, "creditos") if len(df_creditos) > 0 else []
            resultados_debitos = self.analisar_categorias(df_debitos, "debitos") if len(df_debitos) > 0 else []
            
            # Gerar Excel
            print("Gerando Excel...")
            excel_b64 = self.gerar_excel(resultados_gerais, resultados_creditos, resultados_debitos, df, df_creditos, df_debitos)
            
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_transacoes': len(df),
                    'total_debitos': len(df_debitos),
                    'total_creditos': len(df_creditos),
                    'valor_total': float(df['Valor'].sum()),
                    'valor_total_creditos': float(df_creditos['Valor'].sum() if len(df_creditos) > 0 else 0),
                    'valor_total_debitos': float(df_debitos['Valor'].sum() if len(df_debitos) > 0 else 0)
                },
                'categorias_gerais': resultados_gerais,
                'categorias_creditos': resultados_creditos,
                'categorias_debitos': resultados_debitos,
                'excel_file': excel_b64
            }
            
            print("Enviando resposta...")
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(resposta).encode())
            print("=== PROCESSAMENTO CONCLUÍDO ===")
            
        except Exception as e:
            print(f"ERRO: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            
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
    
    def processar_csv(self, csv_data):
        try:
            # Decodificar
            csv_string = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    break
                except:
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível decodificar o CSV")
            
            # Detectar formato
            if ';' in csv_string and 'Data;' in csv_string:
                return self.processar_bradesco(csv_string)
            else:
                return self.processar_bb(csv_string)
                
        except Exception as e:
            raise Exception(f"Erro no CSV: {e}")
    
    def processar_bradesco(self, csv_string):
        try:
            linhas = csv_string.split('\n')
            
            # Encontrar dados
            linha_dados = None
            for linha in linhas:
                if 'Data;Lançamento' in linha and len(linha) > 100:
                    linha_dados = linha
                    break
            
            if not linha_dados:
                raise Exception("Dados do Bradesco não encontrados")
            
            # Separar por \r
            partes = linha_dados.split('\r')
            cabecalho = partes[0]
            dados = [p.strip() for p in partes[1:] if p.strip() and not p.startswith('Total') and re.match(r'^\d{2}/\d{2}/\d{4};', p.strip())]
            
            # Criar DataFrame
            csv_estruturado = cabecalho + '\n' + '\n'.join(dados)
            df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
            
            # Mapear colunas
            mapeamento = {}
            for col in df.columns:
                if 'data' in col.lower():
                    mapeamento[col] = 'Data'
                elif 'lançamento' in col.lower():
                    mapeamento[col] = 'Descricao'
                elif 'crédito' in col.lower():
                    mapeamento[col] = 'Credito'
                elif 'débito' in col.lower():
                    mapeamento[col] = 'Debito'
            
            df = df.rename(columns=mapeamento)
            
            # Processar valores
            df['Credito'] = pd.to_numeric(df.get('Credito', 0), errors='coerce').fillna(0)
            df['Debito'] = pd.to_numeric(df.get('Debito', 0), errors='coerce').fillna(0)
            df['Tipo'] = df.apply(lambda x: 'C' if x['Credito'] > 0 else 'D', axis=1)
            df['Valor'] = df.apply(lambda x: x['Credito'] if x['Credito'] > 0 else x['Debito'], axis=1)
            df['Documento'] = ''
            
            return df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].dropna()
            
        except Exception as e:
            raise Exception(f"Erro no Bradesco: {e}")
    
    def processar_bb(self, csv_string):
        try:
            df = pd.read_csv(io.StringIO(csv_string))
            
            if 'Descrição' in df.columns:
                df['Descricao'] = df['Descrição']
            elif 'Historico' in df.columns:
                df['Descricao'] = df['Historico']
            
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
            df['Documento'] = ''
            
            return df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].dropna()
        except Exception as e:
            raise Exception(f"Erro no BB: {e}")
    
    def categorizar(self, descricao, categorias):
        if not descricao:
            return "Outros"
        
        desc_upper = str(descricao).upper()
        for palavra, categoria in categorias.items():
            if palavra.upper() in desc_upper:
                return categoria
        
        return "Outros"
    
    def analisar_categorias(self, df, tipo):
        if len(df) == 0:
            return []
        
        resultados = df.groupby('Categoria').agg({'Valor': ['sum', 'count']}).reset_index()
        resultados.columns = ['categoria', 'total', 'quantidade']
        valor_total = df['Valor'].sum()
        
        if valor_total > 0:
            resultados['percentual'] = (resultados['total'] / valor_total) * 100
        else:
            resultados['percentual'] = 0
        
        resultados = resultados.sort_values('total', ascending=False)
        
        categorias_lista = []
        for _, row in resultados.iterrows():
            categoria = row['categoria']
            itens_cat = df[df['Categoria'] == categoria]
            
            itens = []
            for _, item in itens_cat.iterrows():
                data_valor = item['Data']
                data_formatada = str(data_valor) if pd.notna(data_valor) else None
                
                itens.append({
                    'data': data_formatada,
                    'descricao': str(item['Descricao']),
                    'valor': float(item['Valor']),
                    'tipo': str(item['Tipo']),
                    'documento': str(item.get('Documento', ''))
                })
            
            categorias_lista.append({
                'categoria': categoria,
                'total': float(row['total']),
                'quantidade': int(row['quantidade']),
                'percentual': float(row['percentual']),
                'itens': itens
            })
        
        return categorias_lista
    
    def gerar_excel(self, categorias_gerais, categorias_creditos, categorias_debitos, df_geral, df_creditos, df_debitos):
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # Aba principal
            ws = wb.create_sheet("Resumo")
            ws.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
            ws.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws.append([])
            
            # Estatísticas
            ws.append(["ESTATÍSTICAS"])
            ws.append(["Total Transações", len(df_geral)])
            ws.append(["Total Créditos", len(df_creditos)])
            ws.append(["Total Débitos", len(df_debitos)])
            ws.append(["Valor Total", f"R$ {df_geral['Valor'].sum():,.2f}"])
            ws.append([])
            ws.append(["Categoria", "Valor", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # Salvar
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            return base64.b64encode(buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None