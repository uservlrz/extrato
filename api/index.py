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
            
            # Verificar se é análise de procedimentos
            action = form_data.get('action', '')
            if action == 'procedures':
                return self.processar_procedimentos(files)
            
            # Análise bancária (código original)
            return self.processar_extratos(files)
            
        except Exception as e:
            self.enviar_erro(str(e))
    
    def processar_procedimentos(self, files):
        """Processa análise de procedimentos médicos"""
        try:
            # Obter arquivos
            procedures_data = files.get('procedures_file')
            categories_data = files.get('categories_file')
            
            if not procedures_data or not categories_data:
                raise Exception("Arquivos 'procedures_file' e 'categories_file' são obrigatórios")
            
            # Processar categorias
            categorias = self.ler_categorias(categories_data)
            
            # Processar procedimentos
            df = self.ler_procedimentos(procedures_data)
            
            # Analisar dados
            analise = self.analisar_procedimentos(df, categorias)
            
            # Gerar Excel
            excel_b64 = self.gerar_excel_procedimentos(analise, df)
            
            # Resposta
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
            self.enviar_erro(f"Erro nos procedimentos: {e}")
    
    def ler_categorias(self, data):
        """Lê arquivo de categorias"""
        df = pd.read_excel(io.BytesIO(data))
        categorias = []
        
        for _, row in df.iterrows():
            if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip():
                categorias.append(str(row.iloc[0]).strip())
        
        return categorias
    
    def ler_procedimentos(self, data):
        """Lê arquivo de procedimentos IPG"""
        # Carregar arquivo
        df = pd.read_excel(io.BytesIO(data))
        
        # Encontrar linha de cabeçalhos (procurar por "Unidade")
        header_row = 2  # Padrão IPG
        for i in range(min(5, len(df))):
            if 'unidade' in str(df.iloc[i, 0]).lower():
                header_row = i
                break
        
        # Recarregar com cabeçalho correto
        df = pd.read_excel(io.BytesIO(data), header=header_row)
        
        # Usar posições fixas da estrutura IPG: Unidade(0), Procedimento(5), Total(10)
        if df.shape[1] >= 11:
            df_proc = df.iloc[:, [0, 5, 10]].copy()
            df_proc.columns = ['Unidade', 'Procedimento', 'TotalItem']
        else:
            raise Exception("Arquivo não tem a estrutura esperada do IPG")
        
        # Limpar dados
        df_proc = df_proc.dropna(subset=['Procedimento', 'TotalItem'])
        df_proc = df_proc[df_proc['Procedimento'].astype(str).str.strip() != '']
        df_proc['TotalItem'] = pd.to_numeric(df_proc['TotalItem'], errors='coerce')
        df_proc = df_proc[df_proc['TotalItem'] > 0]
        df_proc['Unidade'] = df_proc['Unidade'].fillna('Não informado')
        
        return df_proc
    
    def analisar_procedimentos(self, df, categorias):
        """Analisa procedimentos e gera relatórios"""
        # Categorizar procedimentos
        df['Categoria'] = df['Procedimento'].apply(lambda x: self.mapear_categoria(x, categorias))
        
        # Análise por categoria
        cat_agrupadas = df.groupby('Categoria').agg({'TotalItem': ['sum', 'count']}).reset_index()
        cat_agrupadas.columns = ['Categoria', 'Total', 'Quantidade']
        total_geral = df['TotalItem'].sum()
        cat_agrupadas['Percentual'] = (cat_agrupadas['Total'] / total_geral) * 100
        cat_agrupadas = cat_agrupadas.sort_values('Total', ascending=False)
        
        categorias_detalhadas = []
        for _, cat in cat_agrupadas.iterrows():
            procs_cat = df[df['Categoria'] == cat['Categoria']]
            proc_lista = procs_cat.groupby('Procedimento')['TotalItem'].agg(['sum', 'count']).reset_index()
            proc_lista.columns = ['Procedimento', 'Total', 'Quantidade']
            
            categorias_detalhadas.append({
                'categoria': cat['Categoria'],
                'total': float(cat['Total']),
                'quantidade': int(cat['Quantidade']),
                'percentual': float(cat['Percentual']),
                'procedimentos': [{'procedimento': p['Procedimento'], 'total': float(p['Total']), 'quantidade': int(p['Quantidade'])} 
                                for _, p in proc_lista.iterrows()]
            })
        
        # Análise por procedimento
        proc_agrupados = df.groupby('Procedimento').agg({'TotalItem': ['sum', 'count'], 'Categoria': 'first'}).reset_index()
        proc_agrupados.columns = ['Procedimento', 'Total', 'Quantidade', 'Categoria']
        procedimentos_detalhados = [{'procedimento': p['Procedimento'], 'categoria': p['Categoria'], 
                                   'total': float(p['Total']), 'quantidade': int(p['Quantidade'])} 
                                  for _, p in proc_agrupados.iterrows()]
        
        # Análise por unidade
        uni_agrupadas = df.groupby('Unidade').agg({'TotalItem': ['sum', 'count']}).reset_index()
        uni_agrupadas.columns = ['Unidade', 'Total', 'Quantidade']
        uni_agrupadas['Percentual'] = (uni_agrupadas['Total'] / total_geral) * 100
        
        unidades_detalhadas = []
        for _, uni in uni_agrupadas.iterrows():
            cats_uni = df[df['Unidade'] == uni['Unidade']].groupby('Categoria')['TotalItem'].agg(['sum', 'count']).reset_index()
            cats_uni.columns = ['Categoria', 'Total', 'Quantidade']
            
            unidades_detalhadas.append({
                'unidade': uni['Unidade'],
                'total': float(uni['Total']),
                'quantidade': int(uni['Quantidade']),
                'percentual': float(uni['Percentual']),
                'categorias': [{'categoria': c['Categoria'], 'total': float(c['Total']), 'quantidade': int(c['Quantidade'])} 
                             for _, c in cats_uni.iterrows()]
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
    
    def mapear_categoria(self, procedimento, categorias):
        """Mapeia procedimento para categoria"""
        if not procedimento:
            return "Outros"
        
        proc_upper = str(procedimento).upper()
        for categoria in categorias:
            if str(categoria).upper() in proc_upper:
                return categoria
        
        return "Outros"
    
    def gerar_excel_procedimentos(self, analise, df):
        """Gera Excel com relatório de procedimentos"""
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # Aba Resumo
            ws = wb.create_sheet("Resumo")
            ws.append(["ANÁLISE DE PROCEDIMENTOS MÉDICOS"])
            ws.append([f"Total: R$ {analise['estatisticas']['valor_total']:,.2f}"])
            ws.append([])
            ws.append(["Categoria", "Valor", "Quantidade", "Percentual"])
            
            for cat in analise['categorias']:
                ws.append([cat['categoria'], f"R$ {cat['total']:,.2f}", cat['quantidade'], f"{cat['percentual']:.1f}%"])
            
            # Aba Detalhes
            ws2 = wb.create_sheet("Detalhes")
            ws2.append(["Unidade", "Procedimento", "Categoria", "Valor"])
            
            for _, row in df.iterrows():
                ws2.append([row['Unidade'], row['Procedimento'], row['Categoria'], f"R$ {row['TotalItem']:,.2f}"])
            
            # Salvar
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            return base64.b64encode(buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None
    
    def processar_extratos(self, files):
        """Processa análise bancária (código original simplificado)"""
        csv_data = files.get('csv_file')
        excel_data = files.get('excel_file')
        
        if not csv_data or not excel_data:
            raise Exception("Arquivos CSV e Excel são necessários")
        
        # Processar categorias bancárias
        categorias = self.processar_excel_bancario(excel_data)
        
        # Processar CSV bancário
        df = self.processar_csv_bancario(csv_data)
        
        # Categorizar transações
        df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar_bancario(x, categorias))
        
        # Análise simples
        resultados = df.groupby('Categoria').agg({'Valor': ['sum', 'count']}).reset_index()
        resultados.columns = ['categoria', 'total', 'quantidade']
        
        categorias_lista = []
        for _, row in resultados.iterrows():
            itens_cat = df[df['Categoria'] == row['categoria']]
            itens = [{'descricao': item['Descricao'], 'valor': float(item['Valor']), 'tipo': item['Tipo']} 
                    for _, item in itens_cat.iterrows()]
            
            categorias_lista.append({
                'categoria': row['categoria'],
                'total': float(row['total']),
                'quantidade': int(row['quantidade']),
                'itens': itens
            })
        
        resposta = {
            'success': True,
            'categorias_gerais': categorias_lista,
            'excel_file': None
        }
        
        self.enviar_sucesso(resposta)
    
    def processar_excel_bancario(self, data):
        """Processa Excel bancário"""
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
        """Processa CSV bancário (versão simplificada)"""
        # Decodificar
        csv_string = data.decode('utf-8')
        
        # Detectar formato básico
        if ';' in csv_string and 'Data;' in csv_string:
            # Bradesco
            return self.processar_bradesco_simples(csv_string)
        else:
            # Banco do Brasil
            return self.processar_bb_simples(csv_string)
    
    def processar_bradesco_simples(self, csv_string):
        """Processa Bradesco simplificado"""
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
        dados = [p.strip() for p in partes[1:] if p.strip() and not p.startswith('Total')]
        
        # Criar CSV
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
        
        return df[['Data', 'Descricao', 'Valor', 'Tipo']].dropna()
    
    def processar_bb_simples(self, csv_string):
        """Processa Banco do Brasil simplificado"""
        df = pd.read_csv(io.StringIO(csv_string))
        
        if 'Descrição' in df.columns:
            df['Descricao'] = df['Descrição']
        elif 'Historico' in df.columns:
            df['Descricao'] = df['Historico']
        
        df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
        df['Valor'] = df['Valor'].abs()
        
        return df[['Data', 'Descricao', 'Valor', 'Tipo']].dropna()
    
    def categorizar_bancario(self, descricao, categorias):
        """Categoriza transação bancária"""
        if not descricao:
            return "Outros"
        
        desc_upper = str(descricao).upper()
        for palavra, categoria in categorias.items():
            if palavra.upper() in desc_upper:
                return categoria
        
        return "Outros"
    
    def parse_multipart(self, body, boundary):
        """Parse multipart form data"""
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
        """Envia resposta de sucesso"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(dados).encode())
    
    def enviar_erro(self, mensagem):
        """Envia resposta de erro"""
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