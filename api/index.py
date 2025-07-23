from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import io
import base64
import openpyxl
import re
import traceback

class handler(BaseHTTPRequestHandler):
    # ======================
    # HTTP Request Handlers
    # ======================
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
        response = {'status': 'OK', 'message': 'API Unificada funcionando!'}
        self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        try:
            print("=== INICIANDO PROCESSAMENTO ===")
            
            # Read and parse request data
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            content_type = self.headers.get('Content-Type', '')
            
            if 'boundary=' not in content_type:
                raise Exception("Content-Type inválido - boundary não encontrado")
            
            boundary = content_type.split('boundary=')[1]
            files, form_data = self.parse_multipart(post_data, boundary)
            
            # Determine processing type
            action = form_data.get('action', '')
            has_procedures = 'procedures_file' in files and 'categories_file' in files
            has_extratos = 'csv_file' in files and 'excel_file' in files
            
            if action == 'procedures' or has_procedures:
                self.processar_procedimentos(files)
            elif has_extratos:
                self.processar_extratos(files)
            else:
                raise Exception(f"Tipo de análise não identificado. Arquivos recebidos: {list(files.keys())}")
            
        except Exception as e:
            print(f"ERRO GERAL: {str(e)}")
            print(traceback.format_exc())
            self.enviar_erro(str(e))

    # ======================
    # Medical Procedures Processing
    # ======================
    def processar_procedimentos(self, files):
        try:
            procedures_data = files.get('procedures_file')
            categories_data = files.get('categories_file')
            
            if not procedures_data or not categories_data:
                raise Exception("Arquivos necessários não encontrados")
            
            categorias = self.processar_categorias(categories_data)
            df = self.processar_procedimentos_ipg(procedures_data)
            analise = self.analisar_procedimentos(df, categorias)
            excel_b64 = self.gerar_excel_procedimentos(analise, df)
            
            resposta = {
                'success': True,
                'estatisticas': analise['estatisticas'],
                'categorias': analise['categorias'],
                'procedimentos': analise['procedimentos'],
                'unidades': analise['unidades'],
                'excel_file': excel_b64
            }
            
            self.enviar_sucesso(resposta)
            
        except Exception as e:
            print(f"ERRO EM PROCEDIMENTOS: {e}")
            self.enviar_erro(f"Erro nos procedimentos: {e}")
    
    def processar_categorias(self, data):
        try:
            df = pd.read_excel(io.BytesIO(data))
            categorias = []
            
            for i, row in df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip():
                    categoria = str(row.iloc[0]).strip()
                    categorias.append(categoria)
            
            if not categorias:
                raise Exception("Nenhuma categoria válida encontrada")
            
            return categorias
        except Exception as e:
            raise Exception(f"Erro ao processar categorias: {e}")
    
    def processar_procedimentos_ipg(self, data):
        try:
            df = pd.read_excel(io.BytesIO(data))
            
            # Find header row
            header_row = 2  # Default for IPG format
            for i in range(min(5, len(df))):
                if pd.notna(df.iloc[i, 0]):
                    first_cell = str(df.iloc[i, 0]).lower().strip()
                    if 'unidade' in first_cell:
                        header_row = i
                        break
            
            # Reload with correct header
            df = pd.read_excel(io.BytesIO(data), header=header_row)
            
            # Extract relevant columns
            if df.shape[1] >= 11:  # IPG format
                df_proc = df.iloc[:, [0, 5, 10]].copy()
                df_proc.columns = ['Unidade', 'Procedimento', 'TotalItem']
            elif df.shape[1] >= 3:  # Minimal format
                df_proc = df.iloc[:, [0, 1, 2]].copy()
                df_proc.columns = ['Unidade', 'Procedimento', 'TotalItem']
            else:
                raise Exception(f"Arquivo deve ter pelo menos 3 colunas. Encontradas: {df.shape[1]}")
            
            # Clean data
            df_proc = df_proc.dropna(subset=['Procedimento', 'TotalItem'])
            df_proc = df_proc[df_proc['Procedimento'].astype(str).str.strip() != '']
            
            # Convert values
            def converter_valor(valor):
                try:
                    if isinstance(valor, (int, float)):
                        return float(valor)
                    valor_str = str(valor).strip().replace('R$', '').replace('.', '').replace(',', '.').strip()
                    return float(valor_str)
                except:
                    return 0.0
            
            df_proc['TotalItem'] = df_proc['TotalItem'].apply(converter_valor)
            df_proc = df_proc[df_proc['TotalItem'] > 0]
            
            # Clean text fields
            df_proc['Unidade'] = df_proc['Unidade'].fillna('Não informado').astype(str).str.strip()
            df_proc['Procedimento'] = df_proc['Procedimento'].astype(str).str.strip()
            
            if df_proc.empty:
                raise Exception("Nenhum registro válido encontrado após limpeza")
            
            return df_proc
            
        except Exception as e:
            raise Exception(f"Erro ao processar procedimentos: {e}")
    
    def analisar_procedimentos(self, df, categorias):
        try:
            # Categorize procedures
            df['Categoria'] = df['Procedimento'].apply(lambda x: self.mapear_categoria(x, categorias))
            total_geral = df['TotalItem'].sum()
            
            # Analysis by category
            cat_agrupadas = df.groupby('Categoria').agg({'TotalItem': ['sum', 'count']}).reset_index()
            cat_agrupadas.columns = ['Categoria', 'Total', 'Quantidade']
            cat_agrupadas['Percentual'] = (cat_agrupadas['Total'] / total_geral) * 100
            cat_agrupadas = cat_agrupadas.sort_values('Total', ascending=False)
            
            categorias_detalhadas = []
            for _, cat in cat_agrupadas.iterrows():
                procs_cat = df[df['Categoria'] == cat['Categoria']]
                proc_agrupados = procs_cat.groupby('Procedimento')['TotalItem'].agg(['sum', 'count']).reset_index()
                proc_agrupados.columns = ['Procedimento', 'Total', 'Quantidade']
                proc_agrupados = proc_agrupados.sort_values('Total', ascending=False)
                
                procedimentos_lista = [
                    {'procedimento': p['Procedimento'], 'total': float(p['Total']), 'quantidade': int(p['Quantidade'])} 
                    for _, p in proc_agrupados.iterrows()
                ]
                
                categorias_detalhadas.append({
                    'categoria': cat['Categoria'],
                    'total': float(cat['Total']),
                    'quantidade': int(cat['Quantidade']),
                    'percentual': float(cat['Percentual']),
                    'procedimentos': procedimentos_lista
                })
            
            # Analysis by procedure
            proc_agrupados = df.groupby('Procedimento').agg({'TotalItem': ['sum', 'count'], 'Categoria': 'first'}).reset_index()
            proc_agrupados.columns = ['Procedimento', 'Total', 'Quantidade', 'Categoria']
            proc_agrupados = proc_agrupados.sort_values('Total', ascending=False)
            
            procedimentos_detalhados = []
            for _, proc in proc_agrupados.iterrows():
                unidades_proc = df[df['Procedimento'] == proc['Procedimento']].groupby('Unidade')['TotalItem'].agg(['sum', 'count']).reset_index()
                unidades_proc.columns = ['Unidade', 'Total', 'Quantidade']
                
                unidades_dict = {
                    u['Unidade']: {'total': float(u['Total']), 'quantidade': int(u['Quantidade'])} 
                    for _, u in unidades_proc.iterrows()
                }
                
                procedimentos_detalhados.append({
                    'procedimento': proc['Procedimento'],
                    'categoria': proc['Categoria'],
                    'total': float(proc['Total']),
                    'quantidade': int(proc['Quantidade']),
                    'unidades': unidades_dict
                })
            
            # Analysis by unit
            uni_agrupadas = df.groupby('Unidade').agg({'TotalItem': ['sum', 'count']}).reset_index()
            uni_agrupadas.columns = ['Unidade', 'Total', 'Quantidade']
            uni_agrupadas['Percentual'] = (uni_agrupadas['Total'] / total_geral) * 100
            uni_agrupadas = uni_agrupadas.sort_values('Total', ascending=False)
            
            unidades_detalhadas = []
            for _, unidade in uni_agrupadas.iterrows():
                cats_unidade = df[df['Unidade'] == unidade['Unidade']].groupby('Categoria')['TotalItem'].agg(['sum', 'count']).reset_index()
                cats_unidade.columns = ['Categoria', 'Total', 'Quantidade']
                cats_unidade = cats_unidade.sort_values('Total', ascending=False)
                
                categorias_lista = [
                    {'categoria': c['Categoria'], 'total': float(c['Total']), 'quantidade': int(c['Quantidade'])} 
                    for _, c in cats_unidade.iterrows()
                ]
                
                unidades_detalhadas.append({
                    'unidade': unidade['Unidade'],
                    'total': float(unidade['Total']),
                    'quantidade': int(unidade['Quantidade']),
                    'percentual': float(unidade['Percentual']),
                    'categorias': categorias_lista
                })
            
            return {
                'estatisticas': {
                    'total_procedimentos': len(df),
                    'total_categorias': len(cat_agrupadas),
                    'valor_total': float(total_geral),
                    'total_unidades': len(uni_agrupadas)
                },
                'categorias': categorias_detalhadas,
                'procedimentos': procedimentos_detalhados,
                'unidades': unidades_detalhadas
            }
            
        except Exception as e:
            raise Exception(f"Erro na análise: {e}")
    
    def mapear_categoria(self, procedimento, categorias):
        if not procedimento:
            return "Outros"
        
        proc_upper = str(procedimento).upper()
        for categoria in categorias:
            cat_upper = str(categoria).upper().strip()
            if cat_upper in proc_upper:
                return categoria
        
        return "Outros"
    
    def gerar_excel_procedimentos(self, analise, df):
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # Summary sheet
            ws = wb.create_sheet("Resumo")
            ws.append(["ANÁLISE DE PROCEDIMENTOS MÉDICOS IPG"])
            ws.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws.append([])
            
            stats = analise['estatisticas']
            ws.append(["ESTATÍSTICAS"])
            ws.append(["Total Procedimentos", stats['total_procedimentos']])
            ws.append(["Categorias", stats['total_categorias']])
            ws.append(["Valor Total", f"R$ {stats['valor_total']:,.2f}"])
            ws.append(["Unidades", stats['total_unidades']])
            ws.append([])
            ws.append(["Categoria", "Valor", "Quantidade", "Percentual"])
            
            for cat in analise['categorias']:
                ws.append([cat['categoria'], f"R$ {cat['total']:,.2f}", cat['quantidade'], f"{cat['percentual']:.1f}%"])
            
            # Details sheet
            ws2 = wb.create_sheet("Detalhes")
            ws2.append(["Unidade", "Procedimento", "Categoria", "Valor"])
            
            for _, row in df.iterrows():
                ws2.append([row['Unidade'], row['Procedimento'], row['Categoria'], f"R$ {row['TotalItem']:,.2f}"])
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            return base64.b64encode(buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None

    # ======================
    # Bank Statement Processing
    # ======================
    def processar_extratos(self, files):
        try:
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos CSV e Excel são necessários")
            
            categorias = self.processar_excel_bancario(excel_data)
            df = self.processar_csv_bancario(csv_data)
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar_bancario(x, categorias))
            
            # Separate credits and debits
            df_creditos = df[df['Tipo'] == 'C'].copy()
            df_debitos = df[df['Tipo'] == 'D'].copy()
            
            # General analysis
            resultados_gerais = df.groupby('Categoria').agg({'Valor': ['sum', 'count']}).reset_index()
            resultados_gerais.columns = ['categoria', 'total', 'quantidade']
            valor_total = df['Valor'].sum()
            
            resultados_gerais['percentual'] = (resultados_gerais['total'] / valor_total) * 100 if valor_total > 0 else 0
            resultados_gerais = resultados_gerais.sort_values('total', ascending=False)
            
            # Credits analysis
            if len(df_creditos) > 0:
                resultados_creditos = df_creditos.groupby('Categoria').agg({'Valor': ['sum', 'count']}).reset_index()
                resultados_creditos.columns = ['categoria', 'total', 'quantidade']
                valor_total_creditos = df_creditos['Valor'].sum()
                resultados_creditos['percentual'] = (resultados_creditos['total'] / valor_total_creditos) * 100 if valor_total_creditos > 0 else 0
                resultados_creditos = resultados_creditos.sort_values('total', ascending=False)
            else:
                resultados_creditos = pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            # Debits analysis
            if len(df_debitos) > 0:
                resultados_debitos = df_debitos.groupby('Categoria').agg({'Valor': ['sum', 'count']}).reset_index()
                resultados_debitos.columns = ['categoria', 'total', 'quantidade']
                valor_total_debitos = df_debitos['Valor'].sum()
                resultados_debitos['percentual'] = (resultados_debitos['total'] / valor_total_debitos) * 100 if valor_total_debitos > 0 else 0
                resultados_debitos = resultados_debitos.sort_values('total', ascending=False)
            else:
                resultados_debitos = pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            # Prepare detailed categories
            def preparar_categorias_detalhadas(resultados, dataframe):
                categorias_lista = []
                for _, row in resultados.iterrows():
                    categoria = row['categoria']
                    itens_cat = dataframe[dataframe['Categoria'] == categoria]
                    
                    itens = []
                    for _, item in itens_cat.iterrows():
                        itens.append({
                            'data': str(item['Data']) if pd.notna(item['Data']) else None,
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
            
            categorias_gerais = preparar_categorias_detalhadas(resultados_gerais, df)
            categorias_creditos = preparar_categorias_detalhadas(resultados_creditos, df_creditos)
            categorias_debitos = preparar_categorias_detalhadas(resultados_debitos, df_debitos)
            
            # Generate Excel
            excel_b64 = self.gerar_excel_bancario(categorias_gerais, categorias_creditos, categorias_debitos, df, df_creditos, df_debitos)
            
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_transacoes': len(df),
                    'total_debitos': len(df_debitos),
                    'total_creditos': len(df_creditos),
                    'valor_total': float(valor_total),
                    'valor_total_creditos': float(df_creditos['Valor'].sum() if len(df_creditos) > 0 else 0),
                    'valor_total_debitos': float(df_debitos['Valor'].sum() if len(df_debitos) > 0 else 0)
                },
                'categorias_gerais': categorias_gerais,
                'categorias_creditos': categorias_creditos,
                'categorias_debitos': categorias_debitos,
                'excel_file': excel_b64
            }
            
            self.enviar_sucesso(resposta)
            
        except Exception as e:
            self.enviar_erro(f"Erro nos extratos: {e}")
    
    def processar_excel_bancario(self, data):
        df = pd.read_excel(io.BytesIO(data))
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
    
    def processar_csv_bancario(self, data):
        # Try different encodings
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            try:
                csv_string = data.decode(encoding)
                if ';' in csv_string and 'Data;' in csv_string:
                    return self.processar_bradesco_simples(csv_string)
                else:
                    return self.processar_bb_simples(csv_string)
            except:
                continue
        
        raise Exception("Não foi possível decodificar o CSV")
    
    def processar_bradesco_simples(self, csv_string):
        linhas = csv_string.split('\n')
        
        # Find data line
        linha_dados = None
        for linha in linhas:
            if 'Data;Lançamento' in linha and len(linha) > 100:
                linha_dados = linha
                break
        
        if not linha_dados:
            raise Exception("Dados do Bradesco não encontrados")
        
        # Split and filter data
        partes = linha_dados.split('\r')
        cabecalho = partes[0]
        dados = [p.strip() for p in partes[1:] if p.strip() and not p.startswith('Total') and re.match(r'^\d{2}/\d{2}/\d{4};', p.strip())]
        
        # Create DataFrame
        csv_estruturado = cabecalho + '\n' + '\n'.join(dados)
        df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
        
        # Map columns
        mapeamento = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'data' in col_lower:
                mapeamento[col] = 'Data'
            elif 'lançamento' in col_lower:
                mapeamento[col] = 'Descricao'
            elif 'crédito' in col_lower:
                mapeamento[col] = 'Credito'
            elif 'débito' in col_lower:
                mapeamento[col] = 'Debito'
        
        df = df.rename(columns=mapeamento)
        
        # Process values
        df['Credito'] = pd.to_numeric(df.get('Credito', 0), errors='coerce').fillna(0)
        df['Debito'] = pd.to_numeric(df.get('Debito', 0), errors='coerce').fillna(0)
        df['Tipo'] = df.apply(lambda x: 'C' if x['Credito'] > 0 else 'D', axis=1)
        df['Valor'] = df.apply(lambda x: x['Credito'] if x['Credito'] > 0 else x['Debito'], axis=1)
        df['Documento'] = ''
        
        return df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].dropna()
    
    def processar_bb_simples(self, csv_string):
        df = pd.read_csv(io.StringIO(csv_string))
        
        if 'Descrição' in df.columns:
            df['Descricao'] = df['Descrição']
        elif 'Historico' in df.columns:
            df['Descricao'] = df['Historico']
        else:
            raise Exception("Coluna de descrição não encontrada")
        
        df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
        df['Valor'] = df['Valor'].abs()
        df['Documento'] = ''
        
        return df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].dropna()
    
    def categorizar_bancario(self, descricao, categorias):
        if not descricao:
            return "Outros"
        
        desc_upper = str(descricao).upper()
        for palavra, categoria in categorias.items():
            if palavra.upper() in desc_upper:
                return categoria
        
        return "Outros"
    
    def gerar_excel_bancario(self, categorias_gerais, categorias_creditos, categorias_debitos, df_geral, df_creditos, df_debitos):
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # Main sheet
            ws = wb.create_sheet("Resumo")
            ws.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
            ws.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws.append([])
            ws.append(["Categoria", "Valor", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            return base64.b64encode(buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel bancário: {e}")
            return None

    # ======================
    # Helper Functions
    # ======================
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
    
    def enviar_sucesso(self, dados):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(dados).encode())
    
    def enviar_erro(self, mensagem):
        self.send_response(500)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        error_response = {
            'success': False,
            'error': mensagem,
            'traceback': traceback.format_exc()
        }
        self.wfile.write(json.dumps(error_response).encode())