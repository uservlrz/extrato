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
        if pd.isna(valor) or valor == '' or valor is None:
            return 0.0
        
        # Converter para string e limpar espaços
        valor_str = str(valor).strip()
        if not valor_str or valor_str.lower() == 'nan':
            return 0.0
        
        # Remover aspas se houver
        valor_str = valor_str.replace('"', '').replace("'", "")
        
        # Se está vazio após limpeza
        if not valor_str:
            return 0.0
        
        # Tratar valores negativos (podem vir com hífen ou entre aspas negativas)
        negativo = False
        if valor_str.startswith('-') or valor_str.startswith('"-'):
            negativo = True
            valor_str = valor_str.lstrip('-').lstrip('"').rstrip('"')
        
        # Formato brasileiro: remover pontos (milhares) e trocar vírgula por ponto
        # Exemplo: "1.234,56" -> "1234.56"
        if ',' in valor_str and '.' in valor_str:
            # Se tem ambos, ponto é milhares e vírgula é decimal
            valor_str = valor_str.replace('.', '').replace(',', '.')
        elif ',' in valor_str:
            # Só vírgula, é decimal
            valor_str = valor_str.replace(',', '.')
        # Se só tem ponto, assumir que é decimal (formato americano ou sem milhares)
        
        try:
            resultado = float(valor_str)
            # Aplicar sinal negativo se necessário
            return -resultado if negativo else resultado
        except ValueError as e:
            print(f"Erro ao processar valor '{valor}' -> '{valor_str}': {e}")
            return 0.0

    # ==========================================
    # PROCESSAMENTO EXCEL
    # ==========================================
    
    def processar_excel(self, excel_data):
        """Processa arquivo Excel de categorias"""
        try:
            print("=== PROCESSANDO EXCEL ===")
            
            # Verificar formato do arquivo
            formato = self.verificar_formato_excel(excel_data)
            print(f"Formato detectado: {formato}")
            
            if formato == 'xls':
                raise Exception("Arquivos .xls (Excel antigo) não são suportados. Por favor, abra o arquivo no Excel e salve como .xlsx")
            
            # Tentar processar como .xlsx
            try:
                df = pd.read_excel(io.BytesIO(excel_data), engine='openpyxl')
                print("Excel processado com sucesso (.xlsx)")
            except Exception as e1:
                print(f"Erro ao processar Excel: {e1}")
                # Tentar sem especificar engine
                try:
                    df = pd.read_excel(io.BytesIO(excel_data))
                    print("Excel processado com engine padrão")
                except Exception as e2:
                    raise Exception(f"Não foi possível processar o arquivo Excel. Certifique-se de que é um arquivo .xlsx válido. Erro: {e1}")
            
            print(f"Excel carregado: {len(df)} linhas, {len(df.columns)} colunas")
            
            if len(df.columns) < 2:
                raise Exception("Excel deve ter pelo menos 2 colunas (Grupo e Palavra-chave)")
            
            # Normalizar nomes das colunas
            df.columns = ['Grupo', 'Palavra_Chave'] + list(df.columns[2:])
            
            print("Estrutura do Excel:")
            print(f"  Colunas: {list(df.columns)}")
            
            # Processar categorias
            categorias = {}
            categoria_atual = None
            
            for index, row in df.iterrows():
                # Se tem grupo definido, usar como categoria atual
                if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                    categoria_atual = str(row['Grupo']).strip()
                
                # Se tem palavra-chave e categoria atual, adicionar
                if pd.notna(row['Palavra_Chave']) and categoria_atual:
                    palavra = str(row['Palavra_Chave']).strip()
                    if palavra:  # Só adicionar se não estiver vazio
                        categorias[palavra] = categoria_atual
            
            print(f"Total de categorias processadas: {len(categorias)}")
            
            if len(categorias) == 0:
                raise Exception("Nenhuma categoria válida encontrada no Excel. Verifique o formato do arquivo.")
            
            return categorias
            
        except Exception as e:
            print(f"Erro detalhado no Excel: {e}")
            raise Exception(f"Erro no Excel: {e}")
    
    def verificar_formato_excel(self, excel_data):
        """Verifica se é .xls ou .xlsx baseado nos primeiros bytes"""
        # .xlsx começa com PK (ZIP signature)
        if excel_data[:2] == b'PK':
            return 'xlsx'
        # .xls tem assinatura específica
        elif excel_data[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
            return 'xls'
        else:
            return 'unknown'

    # ==========================================
    # PROCESSAMENTO CSV - VERSÃO SIMPLIFICADA
    # ==========================================
    
    def processar_csv(self, csv_data):
        """Processa CSV - versão universal simplificada"""
        try:
            print("=== PROCESSANDO CSV ===")
            
            # Decodificar CSV
            csv_string = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    print(f"CSV decodificado com {encoding}")
                    break
                except:
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível decodificar o CSV")
            
            print(f"Tamanho do arquivo: {len(csv_string)} caracteres")
            
            # Detectar tipo básico (BB vs Bradesco)
            if self.eh_banco_brasil(csv_string):
                print("Formato detectado: Banco do Brasil")
                return self.processar_banco_brasil(csv_string)
            else:
                print("Formato detectado: Bradesco")
                # AQUI ESTÁ A ÚNICA MUDANÇA: CHAMANDO A NOVA FUNÇÃO
                return self.processar_bradesco(csv_string)
                
        except Exception as e:
            print(f"Erro no processamento CSV: {e}")
            raise e
    
    def eh_banco_brasil(self, csv_string):
        """Verifica se é Banco do Brasil"""
        indicadores_bb = [
            '"DATA","DEPENDENCIA ORIGEM"',
            '"DATA","HISTÓRICO"',
            '"DATA","HISTORICO"'
        ]
        
        for indicador in indicadores_bb:
            if indicador in csv_string.upper():
                return True
        
        # Se tem muito mais vírgulas que ponto-vírgulas, provavelmente é BB
        virgulas = csv_string.count(',')
        ponto_virgulas = csv_string.count(';')
        
        return virgulas > ponto_virgulas * 2

    def processar_banco_brasil(self, csv_string):
        """Processa CSV do Banco do Brasil"""
        print("=== PROCESSANDO BANCO DO BRASIL ===")
        
        # Limpar caracteres problemáticos
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        
        df = pd.read_csv(io.StringIO(csv_string))
        
        # Detectar formato e processar
        if 'Descrição' in df.columns or 'Descricao' in df.columns:
            desc_col = 'Descrição' if 'Descrição' in df.columns else 'Descricao'
            df = df.dropna(subset=[desc_col, 'Valor'])
            df['Descricao'] = df[desc_col]
            df['Documento'] = df.get('Documento', '')
        elif 'Historico' in df.columns:
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
        return df

    def processar_bradesco(self, csv_string):
        """
        FUNÇÃO ADICIONADA: Processa extratos do Bradesco, unificando formatos antigo e novo.
        Lida com descrições em múltiplas linhas.
        """
        print("=== PROCESSANDO BRADESCO (LÓGICA UNIFICADA) ===")

        linhas_originais = csv_string.strip().replace('\r', '').split('\n')
        
        # Encontrar o cabeçalho e o início do conteúdo
        header_row = None
        content_start_index = -1
        for i, linha in enumerate(linhas_originais):
            linha_upper = linha.upper()
            if 'DATA;HISTÓRICO' in linha_upper or 'DATA;LANÇAMENTO' in linha_upper:
                header_row = linha
                content_start_index = i + 1
                break

        if not header_row:
            raise Exception("Cabeçalho do extrato Bradesco (Data;Histórico ou Data;Lançamento) não foi encontrado.")

        # Consolidar linhas de descrição (comum no formato novo)
        linhas_consolidadas = []
        i = content_start_index
        while i < len(linhas_originais):
            linha_atual = linhas_originais[i].strip()
            
            # Uma linha de transação válida começa com uma data (dd/mm/yy ou dd/mm/yyyy)
            if re.match(r'^\d{2}/\d{2}/\d{2,4}', linha_atual):
                # Verificar se a próxima linha é uma continuação da descrição
                if (i + 1) < len(linhas_originais):
                    linha_seguinte = linhas_originais[i + 1].strip()
                    if linha_seguinte.startswith(';'):
                        # É uma descrição complementar, anexar à descrição principal
                        descricao_extra = linha_seguinte.replace(';', ' ', 1).strip()
                        partes = linha_atual.split(';')
                        # A descrição é o segundo campo (índice 1)
                        partes[1] = f"{partes[1].strip()} - {descricao_extra}"
                        linha_atual = ';'.join(partes)
                        i += 1 # Pular a linha de descrição extra no próximo loop

                linhas_consolidadas.append(linha_atual)
            i += 1

        if not linhas_consolidadas:
            raise Exception("Nenhuma transação válida encontrada no arquivo Bradesco após o cabeçalho.")

        # Criar DataFrame a partir dos dados limpos
        csv_final = header_row + '\n' + '\n'.join(linhas_consolidadas)
        df = pd.read_csv(io.StringIO(csv_final), sep=';', on_bad_lines='skip')

        # Padronizar nomes das colunas
        df.columns = [col.replace('(R$)', '').replace('.', '').strip().upper() for col in df.columns]
        df = df.rename(columns={'LANÇAMENTO': 'HISTORICO', 'HISTÓRICO': 'HISTORICO', 'DOCTO': 'DOCUMENTO', 'DCTO':'DOCUMENTO', 'CRÉDITO': 'CREDITO', 'DÉBITO': 'DEBITO'})
        
        # Remover transações que não são relevantes (ex: Saldo Anterior)
        df = df[~df['HISTORICO'].str.contains("SALDO ANTERIOR", na=False, case=False)]
        df = df.dropna(subset=['DATA', 'HISTORICO'])

        # Processar valores de crédito e débito
        df['CREDITO_F'] = df['CREDITO'].apply(self.processar_valor_monetario)
        df['DEBITO_F'] = df['DEBITO'].apply(self.processar_valor_monetario)

        # Unificar em uma única coluna de 'Valor' e uma de 'Tipo'
        df['Valor'] = df.apply(lambda row: row['CREDITO_F'] if row['CREDITO_F'] != 0 else abs(row['DEBITO_F']), axis=1)
        df['Tipo'] = df.apply(lambda row: 'C' if row['CREDITO_F'] != 0 else 'D', axis=1)

        # Manter apenas as colunas necessárias e em minúsculas
        df_final = df[['DATA', 'HISTORICO', 'Valor', 'Tipo', 'DOCUMENTO']].copy()
        df_final.columns = ['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']
        
        # Remover linhas sem valor
        df_final = df_final.dropna(subset=['Valor'])
        df_final = df_final[df_final['Valor'] > 0]

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
        
        # Ordenar por tamanho decrescente para matches mais específicos primeiro
        sorted_keys = sorted(categorias.keys(), key=len, reverse=True)
        
        for keyword in sorted_keys:
            if keyword.upper() in desc_upper:
                return categorias[keyword]
        
        return "Outros"

    def gerar_resultados(self, df, df_creditos, df_debitos):
        """Gera todos os resultados agrupados"""
        def agrupar_por_categoria(dataframe, nome_tipo):
            if len(dataframe) == 0:
                return pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            resultados = dataframe.groupby('Categoria').agg({
                'Valor': ['sum', 'count']
            }).reset_index()
            resultados.columns = ['categoria', 'total', 'quantidade']
            
            valor_total = dataframe['Valor'].sum()
            if valor_total > 0:
                resultados['percentual'] = (resultados['total'] / valor_total) * 100
            else:
                resultados['percentual'] = 0
            
            resultados = resultados.sort_values('total', ascending=False)
            return resultados
        
        # Agrupar por categoria
        resultados_gerais = agrupar_por_categoria(df, "Geral")
        resultados_creditos = agrupar_por_categoria(df_creditos, "Créditos")
        resultados_debitos = agrupar_por_categoria(df_debitos, "Débitos")
        
        # Preparar categorias detalhadas
        def preparar_categorias_detalhadas(resultados, dataframe):
            categorias_detalhadas = []
            for _, row in resultados.iterrows():
                categoria = row['categoria']
                itens_cat = dataframe[dataframe['Categoria'] == categoria]
                
                itens = []
                for _, item in itens_cat.iterrows():
                    data_formatada = str(item['Data']) if not pd.isna(item['Data']) else None
                    
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
        
        # Estatísticas
        estatisticas = {
            'total_transacoes': len(df),
            'total_debitos': len(df_debitos),
            'total_creditos': len(df_creditos),
            'valor_total': float(df['Valor'].sum()),
            'valor_total_creditos': float(df_creditos['Valor'].sum() if len(df_creditos) > 0 else 0),
            'valor_total_debitos': float(df_debitos['Valor'].sum() if len(df_debitos) > 0 else 0)
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
            
            # Estatísticas
            total_transacoes = len(df_geral)
            total_debitos = len(df_debitos)
            total_creditos = len(df_creditos)
            valor_total = df_geral['Valor'].sum()
            valor_creditos = df_creditos['Valor'].sum() if len(df_creditos) > 0 else 0
            valor_debitos = df_debitos['Valor'].sum() if len(df_debitos) > 0 else 0
            
            # === ABA RESUMO GERAL ===
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE COMPLETA DE EXTRATO BANCÁRIO"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            ws_resumo.append(["ESTATÍSTICAS GERAIS"])
            ws_resumo.append(["Total de Transações", total_transacoes])
            ws_resumo.append(["Total de Créditos", total_creditos])
            ws_resumo.append(["Total de Débitos", total_debitos])
            ws_resumo.append(["Valor Total Geral", f"R$ {valor_total:,.2f}"])
            ws_resumo.append(["Valor Total Créditos", f"R$ {valor_creditos:,.2f}"])
            ws_resumo.append(["Valor Total Débitos", f"R$ {valor_debitos:,.2f}"])
            ws_resumo.append(["Saldo (Créditos - Débitos)", f"R$ {(valor_creditos - valor_debitos):,.2f}"])
            ws_resumo.append([])
            
            # Resumo por categoria
            ws_resumo.append(["RESUMO GERAL POR CATEGORIA"])
            ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws_resumo.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # Criar abas detalhadas
            def criar_aba_categoria(resultado, prefixo=""):
                categoria = resultado['categoria']
                nome_aba = f"{prefixo}{categoria}".replace('/', '-').replace('\\', '-').replace('*', '-')
                nome_aba = nome_aba.replace('?', '').replace(':', '-').replace('[', '').replace(']', '')
                nome_aba = nome_aba[:31]
                
                ws_categoria = wb.create_sheet(nome_aba)
                ws_categoria.append([f"CATEGORIA: {categoria}"])
                ws_categoria.append([f"Total: R$ {resultado['total']:,.2f}"])
                ws_categoria.append([f"Quantidade: {resultado['quantidade']} itens"])
                ws_categoria.append([])
                ws_categoria.append(["#", "Data", "Descrição", "Valor", "Tipo", "Documento"])
                
                for i, item in enumerate(resultado['itens'], 1):
                    data_formatada = 'Sem data'
                    if item['data']:
                        try:
                            data_formatada = pd.to_datetime(item['data']).strftime('%d/%m/%Y')
                        except:
                            data_formatada = str(item['data'])
                    
                    tipo_formatado = "CRÉDITO" if item['tipo'] == 'C' else "DÉBITO"
                    
                    ws_categoria.append([
                        i, data_formatada, item['descricao'],
                        f"R$ {item['valor']:,.2f}", tipo_formatado, str(item['documento'])
                    ])
            
            # Criar abas para todas as categorias
            for resultado in categorias_gerais:
                criar_aba_categoria(resultado)
            
            # Salvar Excel
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None
