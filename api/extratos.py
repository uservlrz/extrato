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
                raise Exception("Arquivos necessários não foram enviados")
            
            print(f"CSV: {len(csv_data)} bytes")
            print(f"Excel: {len(excel_data)} bytes")
            print("Processando todas as transações (créditos + débitos)")
            
            # Processar Excel
            print("Processando Excel...")
            categorias = self.processar_excel(excel_data)
            print(f"Categorias encontradas: {len(categorias)}")
            
            # Processar CSV - SEMPRE incluir tudo
            print("Processando CSV...")
            df = self.processar_csv(csv_data, incluir_creditos=True)  # Sempre True
            print(f"Linhas processadas: {len(df)}")
            print(f"Colunas: {list(df.columns)}")
            
            # Categorizar
            print("Categorizando transações...")
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar(x, categorias))
            
            # Separar créditos e débitos para análise
            df_creditos = df[df['Tipo'] == 'C'].copy()
            df_debitos = df[df['Tipo'] == 'D'].copy()
            
            print(f"Créditos: {len(df_creditos)} transações")
            print(f"Débitos: {len(df_debitos)} transações")
            
            # Agrupar resultados GERAIS (tudo junto)
            print("Agrupando resultados gerais...")
            resultados_gerais = df.groupby('Categoria').agg({
                'Valor': ['sum', 'count']
            }).reset_index()
            resultados_gerais.columns = ['categoria', 'total', 'quantidade']
            valor_total = df['Valor'].sum()
            
            if valor_total > 0:
                resultados_gerais['percentual'] = (resultados_gerais['total'] / valor_total) * 100
            else:
                resultados_gerais['percentual'] = 0
            resultados_gerais = resultados_gerais.sort_values('total', ascending=False)
            
            # Agrupar resultados CRÉDITOS
            print("Agrupando resultados de créditos...")
            if len(df_creditos) > 0:
                resultados_creditos = df_creditos.groupby('Categoria').agg({
                    'Valor': ['sum', 'count']
                }).reset_index()
                resultados_creditos.columns = ['categoria', 'total', 'quantidade']
                valor_total_creditos = df_creditos['Valor'].sum()
                
                if valor_total_creditos > 0:
                    resultados_creditos['percentual'] = (resultados_creditos['total'] / valor_total_creditos) * 100
                else:
                    resultados_creditos['percentual'] = 0
                resultados_creditos = resultados_creditos.sort_values('total', ascending=False)
            else:
                resultados_creditos = pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            # Agrupar resultados DÉBITOS
            print("Agrupando resultados de débitos...")
            if len(df_debitos) > 0:
                resultados_debitos = df_debitos.groupby('Categoria').agg({
                    'Valor': ['sum', 'count']
                }).reset_index()
                resultados_debitos.columns = ['categoria', 'total', 'quantidade']
                valor_total_debitos = df_debitos['Valor'].sum()
                
                if valor_total_debitos > 0:
                    resultados_debitos['percentual'] = (resultados_debitos['total'] / valor_total_debitos) * 100
                else:
                    resultados_debitos['percentual'] = 0
                resultados_debitos = resultados_debitos.sort_values('total', ascending=False)
            else:
                resultados_debitos = pd.DataFrame(columns=['categoria', 'total', 'quantidade', 'percentual'])
            
            # Função para preparar categorias detalhadas
            def preparar_categorias_detalhadas(resultados, dataframe):
                categorias_detalhadas = []
                for _, row in resultados.iterrows():
                    categoria = row['categoria']
                    itens_cat = dataframe[dataframe['Categoria'] == categoria]
                    
                    itens = []
                    for _, item in itens_cat.iterrows():
                        data_valor = item['Data']
                        if pd.isna(data_valor):
                            data_formatada = None
                        else:
                            data_formatada = str(data_valor)
                        
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
            
            # Preparar respostas
            print("Preparando respostas...")
            categorias_gerais = preparar_categorias_detalhadas(resultados_gerais, df)
            categorias_creditos = preparar_categorias_detalhadas(resultados_creditos, df_creditos)
            categorias_debitos = preparar_categorias_detalhadas(resultados_debitos, df_debitos)
            
            # Gerar Excel
            print("Gerando Excel...")
            excel_b64 = self.gerar_excel_completo(categorias_gerais, categorias_creditos, categorias_debitos, df, df_creditos, df_debitos)
            
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
    
    def detectar_formato_csv(self, csv_string):
        """Detecta se é Banco do Brasil ou Bradesco"""
        try:
            print("=== DETECTANDO FORMATO ===")
            linhas = csv_string.split('\n')
            print(f"Analisando {len(linhas)} linhas...")
            
            for i, linha in enumerate(linhas[:8]):
                linha_mostra = linha[:100] + "..." if len(linha) > 100 else linha
                print(f"Linha {i}: {linha_mostra}")
            
            bradesco_score = 0
            bb_score = 0
            
            for i, linha in enumerate(linhas[:15]):
                linha_upper = linha.upper()
                
                if 'EXTRATO DE:' in linha_upper or 'AGÊNCIA:' in linha_upper or 'CONTA:' in linha_upper:
                    bradesco_score += 3
                    print(f"Bradesco +3 linha {i}: header info")
                
                if 'DATA;LANÇAMENTO;DCTO.' in linha_upper or 'DATA;LAN' in linha_upper or 'DATA;HISTÓRICO' in linha_upper:
                    bradesco_score += 3
                    print(f"Bradesco +3 linha {i}: cabeçalho padrão")
                
                if linha.count('\r') > 5 and ';' in linha:
                    bradesco_score += 2
                    print(f"Bradesco +2 linha {i}: múltiplos \\r")
                
                if re.search(r'\d{2}/\d{2}/\d{4};.*?(PIX|CIELO|TRANSFERENCIA)', linha):
                    bradesco_score += 1
                    print(f"Bradesco +1 linha {i}: padrão transação")
                
                if '"DATA","DEPENDENCIA ORIGEM"' in linha_upper:
                    bb_score += 3
                    print(f"BB +3 linha {i}: cabeçalho dependencia")
                
                if '"DATA"' in linha and ('"HISTÓRICO"' in linha_upper or '"HISTORICO"' in linha_upper) and '","' in linha:
                    bb_score += 3
                    print(f"BB +3 linha {i}: cabeçalho padrão")
                
                if linha.count('","') > 3 and linha.startswith('"'):
                    bb_score += 1
                    print(f"BB +1 linha {i}: formato CSV aspas")
            
            print(f"Bradesco score: {bradesco_score}, BB score: {bb_score}")
            
            if bradesco_score >= bb_score and bradesco_score >= 2:
                print(f"FORMATO BRADESCO DETECTADO (score: {bradesco_score})")
                return 'bradesco'
            elif bb_score >= 2:
                print(f"FORMATO BANCO DO BRASIL DETECTADO (score: {bb_score})")
                return 'banco_brasil'
            
            print("Tentando heurísticas adicionais...")
            
            uso_pontovirgula = sum(linha.count(';') for linha in linhas[:10])
            uso_virgula = sum(linha.count(',') for linha in linhas[:10])
            
            print(f"Uso de ';': {uso_pontovirgula}, Uso de ',': {uso_virgula}")
            
            if uso_pontovirgula > uso_virgula * 1.5:
                print("Formato Bradesco assumido por prevalência de ';'")
                return 'bradesco'
            elif uso_virgula > uso_pontovirgula * 1.5:
                print("Formato Banco do Brasil assumido por prevalência de ','")
                return 'banco_brasil'
            
            print("FORMATO NÃO RECONHECIDO")
            return 'desconhecido'
            
        except Exception as e:
            print(f"Erro na detecção de formato: {e}")
            return 'desconhecido'
    
    def processar_csv_bradesco(self, csv_string, incluir_creditos):
        """
        FUNÇÃO CORRIGIDA: Processa CSV do Bradesco (formatos antigo e novo).
        """
        print("=== PROCESSANDO BRADESCO (LÓGICA CORRIGIDA) ===")
        
        # Função interna robusta para converter valores monetários do Bradesco
        def _processar_valor_bradesco(valor):
            if pd.isna(valor):
                return 0.0
            # Converte para string, remove espaços, aspas e pontos de milhar. Troca vírgula decimal por ponto.
            s = str(valor).strip().replace('"', '').replace('.', '').replace(',', '.')
            if not s:
                return 0.0
            try:
                return float(s)
            except (ValueError, TypeError):
                return 0.0

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

        linhas_consolidadas = []
        i = content_start_index
        while i < len(linhas_originais):
            linha_atual = linhas_originais[i].strip()
            if re.match(r'^\d{2}/\d{2}/\d{2,4}', linha_atual):
                if (i + 1) < len(linhas_originais) and linhas_originais[i + 1].strip().startswith(';'):
                    descricao_extra = linhas_originais[i + 1].strip().replace(';', ' ', 1).strip()
                    partes = linha_atual.split(';')
                    if len(partes) > 1:
                        partes[1] = f"{partes[1].strip()} - {descricao_extra}"
                        linha_atual = ';'.join(partes)
                        i += 1
                linhas_consolidadas.append(linha_atual)
            i += 1

        if not linhas_consolidadas:
            raise Exception("Nenhuma transação válida encontrada no arquivo Bradesco.")

        csv_final = header_row + '\n' + '\n'.join(linhas_consolidadas)
        # Lê o CSV como texto para aplicar a conversão de valor manualmente
        df = pd.read_csv(io.StringIO(csv_final), sep=';', on_bad_lines='skip', dtype=str).fillna('')

        df.columns = [col.replace('(R$)', '').replace('.', '').strip().upper() for col in df.columns]
        df = df.rename(columns={'LANÇAMENTO': 'DESCRICAO', 'HISTÓRICO': 'DESCRICAO', 'DOCTO': 'DOCUMENTO', 'DCTO':'DOCUMENTO', 'CRÉDITO': 'CREDITO', 'DÉBITO': 'DEBITO'})
        
        df = df[~df['DESCRICAO'].str.contains("SALDO ANTERIOR", na=False, case=False)]
        df = df.dropna(subset=['DATA', 'DESCRICAO'])
        
        # Aplica a função de conversão robusta
        df['CREDITO_VALOR'] = df['CREDITO'].apply(_processar_valor_bradesco)
        df['DEBITO_VALOR'] = df['DEBITO'].apply(_processar_valor_bradesco)

        df['Valor'] = df.apply(lambda r: r['CREDITO_VALOR'] if r['CREDITO_VALOR'] != 0 else abs(r['DEBITO_VALOR']), axis=1)
        df['Tipo'] = df.apply(lambda r: 'C' if r['CREDITO_VALOR'] != 0 else 'D', axis=1)

        df_final = df[['DATA', 'DESCRICAO', 'Valor', 'Tipo', 'DOCUMENTO']].copy()
        df_final.columns = ['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']
        
        df_final = df_final[df_final['Valor'] > 0].copy()
        
        try:
            df_final['Data'] = pd.to_datetime(df_final['Data'], format='%d/%m/%Y', errors='coerce')
        except Exception:
            pass # Mantém como string se falhar

        print(f"✅ Bradesco processado com sucesso: {len(df_final)} transações")
        return df_final
    
    def processar_csv_banco_brasil(self, csv_string, incluir_creditos):
        """Processa CSV do Banco do Brasil - SEMPRE retorna tudo"""
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        df = pd.read_csv(io.StringIO(csv_string))
        
        if 'Descrição' in df.columns or 'Descricao' in df.columns:
            desc_col = 'Descrição' if 'Descrição' in df.columns else 'Descricao'
            df = df.dropna(subset=[desc_col, 'Valor'])
            df['Descricao'] = df[desc_col]
            df['Agencia'] = df.get('Agência', df.get('Agencia', ''))
            df['Documento'] = df.get('Documento', '')
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
        elif 'Historico' in df.columns:
            df = df.dropna(subset=['Historico', 'Valor'])
            df = df[df['Historico'] != 'Saldo Anterior']
            df['Descricao'] = df['Historico']
            df['Agencia'] = df.get('Dependencia Origem', '')
            df['Documento'] = df.get('Numero do documento', '')
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
        else:
            raise Exception("Formato de CSV do Banco do Brasil não reconhecido")
        
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df = df.dropna(subset=['Valor'])
        
        print(f"Banco do Brasil - Total final: {len(df)} linhas")
        return df
    
    def processar_csv(self, csv_data, incluir_creditos):
        try:
            print("=== INICIANDO PROCESSAMENTO CSV ===")
            
            csv_string = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    print(f"CSV decodificado com sucesso usando {encoding}")
                    break
                except Exception as e:
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível decodificar o CSV com nenhuma codificação")
            
            formato = self.detectar_formato_csv(csv_string)
            print(f"Formato detectado: {formato}")
            
            if formato == 'bradesco':
                return self.processar_csv_bradesco(csv_string, incluir_creditos)
            elif formato == 'banco_brasil':
                return self.processar_csv_banco_brasil(csv_string, incluir_creditos)
            else:
                raise Exception(f"Formato de CSV não reconhecido.")
                
        except Exception as e:
            print(f"Erro detalhado no processamento CSV: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Erro no CSV: {e}")
    
    def categorizar(self, descricao, categorias):
        if not descricao or pd.isna(descricao):
            return "Outros"
        
        desc_upper = str(descricao).upper()
        
        sorted_keys = sorted(categorias.keys(), key=len, reverse=True)
        
        for keyword in sorted_keys:
            if keyword.upper() in desc_upper:
                return categorias[keyword]
        
        return "Outros"
    
    def gerar_excel_completo(self, categorias_gerais, categorias_creditos, categorias_debitos, df_geral, df_creditos, df_debitos):
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # === ABA RESUMO GERAL ===
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE COMPLETA DE EXTRATO BANCÁRIO"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            total_transacoes = len(df_geral)
            total_debitos = len(df_debitos)
            total_creditos = len(df_creditos)
            valor_total = df_geral['Valor'].sum()
            valor_creditos = df_creditos['Valor'].sum() if len(df_creditos) > 0 else 0
            valor_debitos = df_debitos['Valor'].sum() if len(df_debitos) > 0 else 0
            
            ws_resumo.append(["ESTATÍSTICAS GERAIS"])
            ws_resumo.append(["Total de Transações", total_transacoes])
            ws_resumo.append(["Total de Créditos", total_creditos])
            ws_resumo.append(["Total de Débitos", total_debitos])
            ws_resumo.append(["Valor Total Geral", f"R$ {valor_total:,.2f}"])
            ws_resumo.append(["Valor Total Créditos", f"R$ {valor_creditos:,.2f}"])
            ws_resumo.append(["Valor Total Débitos", f"R$ {valor_debitos:,.2f}"])
            ws_resumo.append(["Saldo (Créditos - Débitos)", f"R$ {(valor_creditos - valor_debitos):,.2f}"])
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
            
            if len(categorias_creditos) > 0:
                ws_creditos = wb.create_sheet("Resumo Créditos")
                ws_creditos.append(["ANÁLISE DE CRÉDITOS (ENTRADAS)"])
                ws_creditos.append([f"Total de Créditos: {total_creditos} transações"])
                ws_creditos.append([f"Valor Total: R$ {valor_creditos:,.2f}"])
                ws_creditos.append([])
                ws_creditos.append(["CRÉDITOS POR CATEGORIA"])
                ws_creditos.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
                for resultado in categorias_creditos:
                    ws_creditos.append([
                        resultado['categoria'],
                        f"R$ {resultado['total']:,.2f}",
                        resultado['quantidade'],
                        f"{resultado['percentual']:.1f}%"
                    ])
            
            if len(categorias_debitos) > 0:
                ws_debitos = wb.create_sheet("Resumo Débitos")
                ws_debitos.append(["ANÁLISE DE DÉBITOS (SAÍDAS)"])
                ws_debitos.append([f"Total de Débitos: {total_debitos} transações"])
                ws_debitos.append([f"Valor Total: R$ {valor_debitos:,.2f}"])
                ws_debitos.append([])
                ws_debitos.append(["DÉBITOS POR CATEGORIA"])
                ws_debitos.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
                for resultado in categorias_debitos:
                    ws_debitos.append([
                        resultado['categoria'],
                        f"R$ {resultado['total']:,.2f}",
                        resultado['quantidade'],
                        f"{resultado['percentual']:.1f}%"
                    ])
            
            def criar_aba_categoria(resultado, prefixo=""):
                categoria = resultado['categoria']
                nome_aba = f"{prefixo}{categoria}".replace('/', '-').replace('\\', '-').replace('*', '-')
                nome_aba = nome_aba.replace('?', '').replace(':', '-').replace('[', '').replace(']', '')
                nome_aba = nome_aba[:31]
                
                ws_categoria = wb.create_sheet(nome_aba)
                ws_categoria.append([f"CATEGORIA: {categoria}"])
                ws_categoria.append([f"Total: R$ {resultado['total']:,.2f}"])
                ws_categoria.append([f"Quantidade: {resultado['quantidade']} itens"])
                ws_categoria.append([f"Percentual: {resultado['percentual']:.1f}% do total"])
                ws_categoria.append([])
                ws_categoria.append(["#", "Data", "Descrição", "Valor", "Tipo", "Documento"])
                
                for i, item in enumerate(resultado['itens'], 1):
                    try:
                        data_formatada = pd.to_datetime(item['data']).strftime('%d/%m/%Y') if item['data'] else 'Sem data'
                    except:
                        data_formatada = str(item['data'])
                    
                    tipo_formatado = "CRÉDITO" if item['tipo'] == 'C' else "DÉBITO"
                    ws_categoria.append([
                        i,
                        data_formatada,
                        item['descricao'],
                        f"R$ {item['valor']:,.2f}",
                        tipo_formatado,
                        str(item['documento'])
                    ])
                
                ws_categoria.append([])
                ws_categoria.append(["", "", "TOTAL DA CATEGORIA:", f"R$ {resultado['total']:,.2f}", "", ""])
                
                ws_categoria.column_dimensions['A'].width = 5
                ws_categoria.column_dimensions['B'].width = 12
                ws_categoria.column_dimensions['C'].width = 50
                ws_categoria.column_dimensions['D'].width = 15
                ws_categoria.column_dimensions['E'].width = 10
                ws_categoria.column_dimensions['F'].width = 15
            
            for resultado in categorias_gerais:
                criar_aba_categoria(resultado)
            
            for resultado in categorias_creditos:
                criar_aba_categoria(resultado, "C_")
            
            for resultado in categorias_debitos:
                criar_aba_categoria(resultado, "D_")
            
            for ws in [ws_resumo]:
                ws.column_dimensions['A'].width = 25
                ws.column_dimensions['B'].width = 15
                ws.column_dimensions['C'].width = 12
                ws.column_dimensions['D'].width = 12
            
            if 'ws_creditos' in locals():
                ws_creditos.column_dimensions['A'].width = 25
                ws_creditos.column_dimensions['B'].width = 15
                ws_creditos.column_dimensions['C'].width = 12
                ws_creditos.column_dimensions['D'].width = 12
            
            if 'ws_debitos' in locals():
                ws_debitos.column_dimensions['A'].width = 25
                ws_debitos.column_dimensions['B'].width = 15
                ws_debitos.column_dimensions['C'].width = 12
                ws_debitos.column_dimensions['D'].width = 12
            
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None
