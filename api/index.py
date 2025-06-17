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
            incluir_creditos = form_data.get('incluir_creditos', 'false') == 'true'
            
            if not csv_data or not excel_data:
                raise Exception("Arquivos necessários não foram enviados")
            
            print(f"CSV: {len(csv_data)} bytes")
            print(f"Excel: {len(excel_data)} bytes")
            print(f"Incluir créditos: {incluir_creditos}")
            
            # Processar Excel
            print("Processando Excel...")
            categorias = self.processar_excel(excel_data)
            print(f"Categorias encontradas: {len(categorias)}")
            
            # Processar CSV (agora detecta automaticamente o formato)
            print("Processando CSV...")
            df = self.processar_csv(csv_data, incluir_creditos)
            print(f"Linhas processadas: {len(df)}")
            print(f"Colunas: {list(df.columns)}")
            
            # Categorizar
            print("Categorizando transações...")
            df['Categoria'] = df['Descricao'].apply(lambda x: self.categorizar(x, categorias))
            
            # Agrupar resultados
            print("Agrupando resultados...")
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
            print("Preparando resposta...")
            categorias_detalhadas = []
            for _, row in resultados.iterrows():
                categoria = row['categoria']
                itens_cat = df[df['Categoria'] == categoria]
                
                itens = []
                for _, item in itens_cat.iterrows():
                    # Tratar valores None/NaN na data
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
            
            # Gerar Excel
            print("Gerando Excel...")
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
            
            # Mostrar primeiras linhas para debug
            for i, linha in enumerate(linhas[:8]):
                print(f"Linha {i}: {linha[:100]}...")
            
            # Verificar se é Bradesco
            for i, linha in enumerate(linhas[:10]):
                if 'Extrato de:' in linha or 'Agência:' in linha:
                    print(f"Bradesco detectado na linha {i}: padrão 'Extrato de:' ou 'Agência:'")
                    return 'bradesco'
                if 'Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha:
                    print(f"Bradesco detectado na linha {i}: cabeçalho com ';'")
                    return 'bradesco'
                if ';' in linha and 'Lançamento' in linha:
                    print(f"Bradesco detectado na linha {i}: contém ';' e 'Lançamento'")
                    return 'bradesco'
            
            # Verificar se é Banco do Brasil
            for i, linha in enumerate(linhas[:5]):
                if 'Data","Dependencia Origem","Hist' in linha or 'Data","Dependencia Origem","Historico"' in linha:
                    print(f"Banco do Brasil detectado na linha {i}: padrão dependencia origem")
                    return 'banco_brasil'
                if '"Data",' in linha and '"Histórico",' in linha:
                    print(f"Banco do Brasil detectado na linha {i}: padrão com aspas e vírgulas")
                    return 'banco_brasil'
                if '"Data",' in linha and ('"Historico",' in linha or '"Hist' in linha):
                    print(f"Banco do Brasil detectado na linha {i}: variação do padrão histórico")
                    return 'banco_brasil'
            
            print("Formato não reconhecido pelos padrões principais")
            return 'desconhecido'
            
        except Exception as e:
            print(f"Erro na detecção de formato: {e}")
            return 'desconhecido'
    
    def processar_csv_bradesco(self, csv_string, incluir_creditos):
        """Processa CSV do Bradesco"""
        try:
            print("Processando CSV do Bradesco...")
            linhas = csv_string.split('\n')
            print(f"Total de linhas: {len(linhas)}")
            
            # Mostrar primeiras linhas para debug
            for i, linha in enumerate(linhas[:10]):
                print(f"Linha {i}: {linha[:100]}...")
            
            # Encontrar onde começam os dados reais
            inicio_dados = -1
            for i, linha in enumerate(linhas):
                if 'Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha:
                    inicio_dados = i
                    print(f"Dados encontrados na linha {i}")
                    break
            
            if inicio_dados == -1:
                raise Exception("Não foi possível encontrar o cabeçalho dos dados no CSV do Bradesco")
            
            # Extrair apenas a parte dos dados
            dados_texto = '\n'.join(linhas[inicio_dados:])
            print(f"Dados extraídos: {len(dados_texto)} caracteres")
            
            # Limpar linhas vazias e totais
            linhas_dados = []
            for linha in dados_texto.split('\n'):
                linha_limpa = linha.strip()
                if (linha_limpa and 
                    not linha_limpa.startswith('Total;') and 
                    'SALDO ANTERIOR' not in linha_limpa and
                    not linha_limpa.startswith(';') and
                    linha_limpa != 'Data;Lançamento;Dcto.;Crédito (R$);Débito (R$);Saldo (R$)' and
                    'Data;Lan' not in linha_limpa):
                    linhas_dados.append(linha_limpa)
            
            print(f"Linhas de dados filtradas: {len(linhas_dados)}")
            
            if len(linhas_dados) == 0:
                raise Exception("Nenhuma linha de dados válida encontrada")
            
            # Adicionar cabeçalho padronizado
            cabecalho = "Data;Lançamento;Dcto.;Crédito (R$);Débito (R$);Saldo (R$)"
            csv_limpo = cabecalho + '\n' + '\n'.join(linhas_dados)
            
            print("Tentando ler com pandas...")
            # Ler com pandas
            df = pd.read_csv(io.StringIO(csv_limpo), delimiter=';')
            print(f"DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
            
            # Renomear colunas para padronizar
            colunas_map = {
                'Data': 'Data',
                'Lançamento': 'Descricao',
                'Dcto.': 'Documento',
                'Crédito (R$)': 'Credito',
                'Débito (R$)': 'Debito',
                'Saldo (R$)': 'Saldo'
            }
            
            # Renomear colunas que existem
            for col_antiga, col_nova in colunas_map.items():
                if col_antiga in df.columns:
                    df = df.rename(columns={col_antiga: col_nova})
            
            print(f"Colunas após renomeação: {list(df.columns)}")
            
            # Processar valores monetários
            def processar_valor_bradesco(valor):
                if pd.isna(valor) or valor == '' or valor is None:
                    return 0.0
                valor_str = str(valor).replace('.', '').replace(',', '.')
                try:
                    return float(valor_str)
                except Exception as e:
                    print(f"Erro ao processar valor '{valor}': {e}")
                    return 0.0
            
            # Aplicar processamento de valores
            if 'Credito' in df.columns:
                df['Credito'] = df['Credito'].apply(processar_valor_bradesco)
            else:
                df['Credito'] = 0.0
                
            if 'Debito' in df.columns:
                df['Debito'] = df['Debito'].apply(processar_valor_bradesco)
            else:
                df['Debito'] = 0.0
            
            # Criar coluna Valor e Tipo
            df['Valor'] = df['Credito'] + df['Debito']
            df['Tipo'] = df.apply(lambda row: 'C' if row['Credito'] > 0 else 'D', axis=1)
            
            print(f"Valores processados - Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
            
            # Filtrar créditos se necessário
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']
                print(f"Após filtrar créditos: {len(df)} linhas")
            
            # Limpar dados
            df = df.dropna(subset=['Descricao'])
            df = df[df['Valor'] > 0]
            
            print(f"Após limpeza: {len(df)} linhas")
            
            # Adicionar colunas ausentes
            if 'Documento' not in df.columns:
                df['Documento'] = ''
            
            # Processar datas
            try:
                df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
            except Exception as e:
                print(f"Erro ao processar datas: {e}")
                # Manter como string se não conseguir converter
            
            resultado = df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].reset_index(drop=True)
            print(f"Resultado final: {len(resultado)} linhas")
            return resultado
            
        except Exception as e:
            print(f"Erro detalhado no processamento Bradesco: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Erro no processamento CSV Bradesco: {e}")
    
    def processar_csv_banco_brasil(self, csv_string, incluir_creditos):
        """Processa CSV do Banco do Brasil (código original)"""
        # Limpar caracteres problemáticos
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
            raise Exception("Formato de CSV do Banco do Brasil não reconhecido")
        
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df = df.dropna(subset=['Valor'])
        
        return df
    
    def processar_csv(self, csv_data, incluir_creditos):
        try:
            print("=== INICIANDO PROCESSAMENTO CSV ===")
            
            # Tentar diferentes codificações
            csv_string = None
            encoding_usado = None
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    csv_string = csv_data.decode(encoding)
                    encoding_usado = encoding
                    print(f"CSV decodificado com sucesso usando {encoding}")
                    break
                except Exception as e:
                    print(f"Falha ao decodificar com {encoding}: {e}")
                    continue
            
            if not csv_string:
                raise Exception("Não foi possível decodificar o CSV com nenhuma codificação")
            
            print(f"CSV decodificado: {len(csv_string)} caracteres")
            print(f"Primeiros 500 caracteres: {csv_string[:500]}")
            
            # Detectar formato
            formato = self.detectar_formato_csv(csv_string)
            print(f"Formato detectado: {formato}")
            
            if formato == 'bradesco':
                return self.processar_csv_bradesco(csv_string, incluir_creditos)
            elif formato == 'banco_brasil':
                return self.processar_csv_banco_brasil(csv_string, incluir_creditos)
            else:
                raise Exception(f"Formato de CSV não reconhecido. Formato detectado: {formato}. Formatos suportados: Banco do Brasil e Bradesco.")
                
        except Exception as e:
            print(f"Erro detalhado no processamento CSV: {e}")
            print(f"Traceback: {traceback.format_exc()}")
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
            wb.remove(wb.active)
            
            # Aba Resumo
            ws_resumo = wb.create_sheet("Resumo")
            ws_resumo.append(["ANÁLISE DE EXTRATO BANCÁRIO"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            # Estatísticas gerais
            total_transacoes = len(df)
            total_debitos = len(df[df['Tipo'] == 'D'])
            total_creditos = len(df[df['Tipo'] == 'C'])
            valor_total = df['Valor'].sum()
            
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
                ws_resumo.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # Criar aba para cada categoria com itens detalhados
            for resultado in resultados:
                categoria = resultado['categoria']
                
                # Nome da aba (máximo 31 caracteres, sem caracteres especiais)
                nome_aba = categoria.replace('/', '-').replace('\\', '-').replace('*', '-')
                nome_aba = nome_aba.replace('?', '').replace(':', '-').replace('[', '').replace(']', '')
                nome_aba = nome_aba[:31]  # Limite do Excel
                
                ws_categoria = wb.create_sheet(nome_aba)
                
                # Cabeçalho da categoria
                ws_categoria.append([f"CATEGORIA: {categoria}"])
                ws_categoria.append([f"Total: R$ {resultado['total']:,.2f}"])
                ws_categoria.append([f"Quantidade: {resultado['quantidade']} itens"])
                ws_categoria.append([f"Percentual: {resultado['percentual']:.1f}% do total"])
                ws_categoria.append([])
                
                # Cabeçalho da tabela de itens
                ws_categoria.append(["#", "Data", "Descrição", "Valor", "Tipo", "Documento"])
                
                # Itens da categoria
                for i, item in enumerate(resultado['itens'], 1):
                    # Formatar data
                    if item['data']:
                        try:
                            if isinstance(item['data'], str):
                                data_formatada = pd.to_datetime(item['data'], dayfirst=True).strftime('%d/%m/%Y')
                            else:
                                data_formatada = item['data'].strftime('%d/%m/%Y')
                        except:
                            data_formatada = str(item['data'])
                    else:
                        data_formatada = 'Sem data'
                    
                    # Formatar tipo
                    tipo_formatado = "CRÉDITO" if item['tipo'] == 'C' else "DÉBITO"
                    
                    ws_categoria.append([
                        i,
                        data_formatada,
                        item['descricao'],
                        f"R$ {item['valor']:,.2f}",
                        tipo_formatado,
                        str(item['documento'])
                    ])
                
                # Total da categoria
                ws_categoria.append([])
                ws_categoria.append(["", "", "TOTAL DA CATEGORIA:", f"R$ {resultado['total']:,.2f}", "", ""])
                
                # Ajustar largura das colunas
                ws_categoria.column_dimensions['A'].width = 5
                ws_categoria.column_dimensions['B'].width = 12
                ws_categoria.column_dimensions['C'].width = 50
                ws_categoria.column_dimensions['D'].width = 15
                ws_categoria.column_dimensions['E'].width = 10
                ws_categoria.column_dimensions['F'].width = 15
            
            # Ajustar largura das colunas do resumo
            ws_resumo.column_dimensions['A'].width = 25
            ws_resumo.column_dimensions['B'].width = 15
            ws_resumo.column_dimensions['C'].width = 12
            ws_resumo.column_dimensions['D'].width = 12
            
            # Salvar
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None