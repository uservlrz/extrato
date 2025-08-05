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
            print("=== INICIANDO PROCESSAMENTO ===")
            
            # Receber dados
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            # Parse multipart
            content_type = self.headers.get('Content-Type', '')
            if 'boundary=' not in content_type:
                raise Exception("Content-Type inválido - boundary não encontrado")
            
            boundary = content_type.split('boundary=')[1]
            files, form_data = self.parse_multipart(post_data, boundary)
            
            csv_data = files.get('csv_file')
            excel_data = files.get('excel_file')
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos necessários não foram enviados")
            
            print(f"CSV: {len(csv_data)} bytes, Excel: {len(excel_data)} bytes")
            
            # Processar arquivos
            categorias = self.processar_excel(excel_data)
            df = self.processar_csv(csv_data)
            
            # Categorizar transações
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar(x, categorias))
            
            # Separar por tipo
            df_creditos = df[df['Tipo'] == 'C'].copy()
            df_debitos = df[df['Tipo'] == 'D'].copy()
            
            # Gerar resultados
            resultados = self.gerar_resultados(df, df_creditos, df_debitos)
            
            # Gerar Excel
            excel_b64 = self.gerar_excel_completo(
                resultados['categorias_gerais'], 
                resultados['categorias_creditos'], 
                resultados['categorias_debitos'], 
                df, df_creditos, df_debitos
            )
            
            # Resposta final
            resposta = {
                'success': True,
                'estatisticas': resultados['estatisticas'],
                'categorias_gerais': resultados['categorias_gerais'],
                'categorias_creditos': resultados['categorias_creditos'],
                'categorias_debitos': resultados['categorias_debitos'],
                'excel_file': excel_b64
            }
            
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

    # ==========================================
    # UTILITÁRIOS
    # ==========================================
    
    def parse_multipart(self, body, boundary):
        """Parse de dados multipart/form-data"""
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

    def processar_valor_monetario(self, valor):
        """Converte valores monetários brasileiros para float"""
        if pd.isna(valor):
            return 0.0
        
        valor_str = str(valor).strip().replace('"', '')
        if not valor_str or valor_str.lower() == 'nan':
            return 0.0
        
        # Lógica para tratar formatos como "1.234,56" ou "-1.234,56"
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            return float(valor_str)
        except (ValueError, TypeError):
            return 0.0

    # ==========================================
    # PROCESSAMENTO EXCEL
    # ==========================================
    
    def processar_excel(self, excel_data):
        """Processa arquivo Excel de categorias"""
        try:
            print("=== PROCESSANDO EXCEL ===")
            df = pd.read_excel(io.BytesIO(excel_data), engine='openpyxl')
            
            if len(df.columns) < 2:
                raise Exception("Excel deve ter pelo menos 2 colunas (Grupo e Palavra-chave)")
            
            df.columns = ['Grupo', 'Palavra_Chave'] + list(df.columns[2:])
            
            categorias = {}
            categoria_atual = None
            
            for index, row in df.iterrows():
                if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                    categoria_atual = str(row['Grupo']).strip()
                
                if pd.notna(row['Palavra_Chave']) and categoria_atual:
                    palavra = str(row['Palavra_Chave']).strip()
                    if palavra:
                        categorias[palavra] = categoria_atual
            
            if not categorias:
                raise Exception("Nenhuma categoria válida encontrada no Excel.")
            
            print(f"Total de categorias processadas: {len(categorias)}")
            return categorias
        except Exception as e:
            raise Exception(f"Erro no Excel: {e}")
    
    # ==========================================
    # PROCESSAMENTO CSV
    # ==========================================
    
    def processar_csv(self, csv_data):
        """Processa CSV, detectando o banco e chamando a função correta."""
        try:
            print("=== PROCESSANDO CSV ===")
            csv_string = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    print(f"CSV decodificado com {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível decodificar o CSV")
            
            if self.eh_banco_brasil(csv_string):
                print("Formato detectado: Banco do Brasil")
                return self.processar_banco_brasil(csv_string)
            else:
                print("Formato detectado: Bradesco")
                return self.processar_bradesco(csv_string)
        except Exception as e:
            print(f"Erro no processamento CSV: {e}")
            raise e
    
    def eh_banco_brasil(self, csv_string):
        """Verifica se o CSV é do Banco do Brasil."""
        indicadores_bb = ['"DATA","DEPENDENCIA ORIGEM"', '"DATA","HISTÓRICO"', '"DATA","HISTORICO"']
        csv_upper = csv_string.upper()
        if any(indicador in csv_upper for indicador in indicadores_bb):
            return True
        return csv_string.count(',') > csv_string.count(';') * 2

    def processar_banco_brasil(self, csv_string):
        """Processa CSV do Banco do Brasil."""
        print("=== PROCESSANDO BANCO DO BRASIL ===")
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        df = pd.read_csv(io.StringIO(csv_string))
        
        if 'Historico' in df.columns:
            df = df.dropna(subset=['Historico', 'Valor'])
            df = df[df['Historico'] != 'Saldo Anterior']
            df['Descricao'] = df['Historico']
            df['Documento'] = df.get('Numero do documento', '')
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
        else:
            raise Exception("Formato de CSV do Banco do Brasil não reconhecido")
        
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df = df.dropna(subset=['Valor'])
        print(f"Banco do Brasil processado: {len(df)} linhas")
        return df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']]

    def processar_bradesco(self, csv_string):
        """
        NOVA FUNÇÃO: Processa extratos do Bradesco, unificando formatos antigo e novo.
        Lida com descrições em múltiplas linhas.
        """
        print("=== PROCESSANDO BRADESCO (FORMATO ANTIGO E NOVO) ===")
        linhas_originais = csv_string.strip().replace('\r', '').split('\n')
        
        header_row, content_start_index = None, -1
        for i, linha in enumerate(linhas_originais):
            linha_upper = linha.upper()
            if 'DATA;HISTÓRICO' in linha_upper or 'DATA;LANÇAMENTO' in linha_upper:
                header_row = linha
                content_start_index = i + 1
                break

        if not header_row:
            raise Exception("Cabeçalho do extrato Bradesco (Data;Histórico ou Data;Lançamento) não foi encontrado.")

        # Consolida linhas de descrição (comum no formato novo)
        linhas_consolidadas = []
        i = content_start_index
        while i < len(linhas_originais):
            linha_atual = linhas_originais[i].strip()
            if re.match(r'^\d{2}/\d{2}/\d{2,4}', linha_atual):
                # Verifica se a próxima linha é uma continuação da descrição
                if (i + 1) < len(linhas_originais) and linhas_originais[i + 1].strip().startswith(';'):
                    descricao_extra = linhas_originais[i + 1].strip().replace(';', ' ', 1).strip()
                    partes = linha_atual.split(';')
                    partes[1] = f"{partes[1].strip()} - {descricao_extra}"
                    linha_atual = ';'.join(partes)
                    i += 1 # Pula a linha de descrição extra
                linhas_consolidadas.append(linha_atual)
            i += 1

        if not linhas_consolidadas:
            raise Exception("Nenhuma transação válida encontrada no arquivo Bradesco.")

        csv_final = header_row + '\n' + '\n'.join(linhas_consolidadas)
        df = pd.read_csv(io.StringIO(csv_final), sep=';', on_bad_lines='skip')

        # Padroniza nomes das colunas
        df.columns = [col.replace('(R$)', '').replace('.', '').strip().upper() for col in df.columns]
        df = df.rename(columns={'LANÇAMENTO': 'HISTORICO', 'DOCTO': 'DOCUMENTO', 'DCTO': 'DOCUMENTO', 'CRÉDITO': 'CREDITO', 'DÉBITO': 'DEBITO'})
        
        df = df[~df['HISTORICO'].str.contains("SALDO ANTERIOR", na=False, case=False)]
        df = df.dropna(subset=['DATA', 'HISTORICO'])

        # Processa valores de crédito e débito
        df['CREDITO_F'] = df['CREDITO'].apply(self.processar_valor_monetario)
        df['DEBITO_F'] = df['DEBITO'].apply(self.processar_valor_monetario)

        # Unifica em 'Valor' e 'Tipo'
        df['Valor'] = df.apply(lambda r: r['CREDITO_F'] if r['CREDITO_F'] != 0 else abs(r['DEBITO_F']), axis=1)
        df['Tipo'] = df.apply(lambda r: 'C' if r['CREDITO_F'] != 0 else 'D', axis=1)

        df_final = df[['DATA', 'HISTORICO', 'Valor', 'Tipo', 'DOCUMENTO']].rename(columns={'DATA': 'Data', 'HISTORICO': 'Descricao', 'DOCUMENTO': 'Documento'})
        df_final = df_final[df_final['Valor'] > 0].copy()
        
        print(f"✅ Bradesco processado com sucesso: {len(df_final)} transações")
        return df_final

    # ==========================================
    # CATEGORIZAÇÃO E RESULTADOS
    # ==========================================
    
    def categorizar(self, descricao, categorias):
        """Categoriza uma descrição baseada nas palavras-chave"""
        if not descricao or pd.isna(descricao):
            return "Outros"
        
        desc_upper = str(descricao).upper()
        sorted_keys = sorted(categorias.keys(), key=len, reverse=True)
        
        for keyword in sorted_keys:
            if keyword.upper() in desc_upper:
                return categorias[keyword]
        
        return "Outros"

    def gerar_resultados(self, df, df_creditos, df_debitos):
        """Gera todos os resultados agrupados"""
        def agrupar_por_categoria(dataframe, nome_tipo):
            if dataframe.empty:
                return pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            resultados = dataframe.groupby('Categoria').agg(total=('Valor', 'sum'), quantidade=('Valor', 'count')).reset_index()
            valor_total = dataframe['Valor'].sum()
            resultados['percentual'] = (resultados['total'] / valor_total * 100) if valor_total > 0 else 0
            return resultados.sort_values('total', ascending=False)
        
        def preparar_categorias_detalhadas(resultados, dataframe):
            categorias_detalhadas = []
            for _, row in resultados.iterrows():
                categoria = row['categoria']
                itens_cat = dataframe[dataframe['Categoria'] == categoria]
                
                itens = []
                for _, item in itens_cat.iterrows():
                    data_formatada = str(item['Data']) if pd.notna(item['Data']) else None
                    itens.append({
                        'data': data_formatada,
                        'descricao': str(item['Descricao']),
                        'valor': float(item['Valor']),
                        'tipo': str(item['Tipo']),
                        'documento': str(item.get('Documento', ''))
                    })
                
                categorias_detalhadas.append({
                    'categoria': categoria,
                    'total': float(row['total']),
                    'quantidade': int(row['quantidade']),
                    'percentual': float(row['percentual']),
                    'itens': itens
                })
            return categorias_detalhadas
        
        # Agrupar por categoria
        resultados_gerais = agrupar_por_categoria(df, "Geral")
        resultados_creditos = agrupar_por_categoria(df_creditos, "Créditos")
        resultados_debitos = agrupar_por_categoria(df_debitos, "Débitos")
        
        estatisticas = {
            'total_transacoes': len(df),
            'total_debitos': len(df_debitos),
            'total_creditos': len(df_creditos),
            'valor_total': float(df['Valor'].sum()),
            'valor_total_creditos': float(df_creditos['Valor'].sum()),
            'valor_total_debitos': float(df_debitos['Valor'].sum())
        }
        
        return {
            'estatisticas': estatisticas,
            'categorias_gerais': preparar_categorias_detalhadas(resultados_gerais, df),
            'categorias_creditos': preparar_categorias_detalhadas(resultados_creditos, df_creditos),
            'categorias_debitos': preparar_categorias_detalhadas(resultados_debitos, df_debitos)
        }

    # ==========================================
    # GERAÇÃO DE EXCEL
    # ==========================================
    
    def gerar_excel_completo(self, categorias_gerais, categorias_creditos, categorias_debitos, df_geral, df_creditos, df_debitos):
        """Gera Excel completo com todas as abas"""
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            stats = self.gerar_resultados(df_geral, df_creditos, df_debitos)['estatisticas']

            # === ABA RESUMO GERAL ===
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE COMPLETA DE EXTRATO BANCÁRIO"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            ws_resumo.append(["ESTATÍSTICAS GERAIS"])
            ws_resumo.append(["Total de Transações", stats['total_transacoes']])
            ws_resumo.append(["Total de Créditos", stats['total_creditos']])
            ws_resumo.append(["Total de Débitos", stats['total_debitos']])
            ws_resumo.append(["Valor Total Créditos", f"R$ {stats['valor_total_creditos']:,.2f}"])
            ws_resumo.append(["Valor Total Débitos", f"R$ {stats['valor_total_debitos']:,.2f}"])
            ws_resumo.append(["Saldo (Créditos - Débitos)", f"R$ {(stats['valor_total_creditos'] - stats['valor_total_debitos']):,.2f}"])
            ws_resumo.append([])
            
            ws_resumo.append(["RESUMO GERAL POR CATEGORIA"])
            ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws_resumo.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # --- ABAS DETALHADAS ---
            def criar_aba_categoria(resultado, prefixo=""):
                nome_aba = re.sub(r'[\\/*?:\[\]]', '', f"{prefixo}{resultado['categoria']}")[:31]
                ws_categoria = wb.create_sheet(nome_aba)
                ws_categoria.append([f"CATEGORIA: {categoria}"])
                ws_categoria.append([f"Total: R$ {resultado['total']:,.2f}"])
                ws_categoria.append([f"Quantidade: {resultado['quantidade']} itens"])
                ws_categoria.append([])
                ws_categoria.append(["Data", "Descrição", "Valor", "Tipo", "Documento"])
                
                for item in resultado['itens']:
                    data_formatada = 'Sem data'
                    if item['data']:
                        try:
                            data_formatada = pd.to_datetime(item['data']).strftime('%d/%m/%Y')
                        except:
                            data_formatada = str(item['data'])
                    
                    tipo_formatado = "CRÉDITO" if item['tipo'] == 'C' else "DÉBITO"
                    ws_categoria.append([
                        data_formatada, item['descricao'], f"R$ {item['valor']:,.2f}", tipo_formatado, str(item['documento'])
                    ])
            
            for resultado in categorias_gerais:
                criar_aba_categoria(resultado)
            
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None
