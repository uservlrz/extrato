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
                linha_mostra = linha[:100] + "..." if len(linha) > 100 else linha
                print(f"Linha {i}: {linha_mostra}")
            
            bradesco_score = 0
            bb_score = 0
            
            # Analisar cada linha
            for i, linha in enumerate(linhas[:15]):
                linha_upper = linha.upper()
                
                # Indicadores do Bradesco
                if 'EXTRATO DE:' in linha_upper or 'AGÊNCIA:' in linha_upper or 'CONTA:' in linha_upper:
                    bradesco_score += 3
                    print(f"Bradesco +3 linha {i}: header info")
                
                if 'DATA;LANÇAMENTO;DCTO.' in linha_upper or 'DATA;LAN' in linha_upper:
                    bradesco_score += 3
                    print(f"Bradesco +3 linha {i}: cabeçalho padrão")
                
                if linha.count('\r') > 5 and ';' in linha:
                    bradesco_score += 2
                    print(f"Bradesco +2 linha {i}: múltiplos \\r")
                
                if re.search(r'\d{2}/\d{2}/\d{4};.*?(PIX|CIELO|TRANSFERENCIA)', linha):
                    bradesco_score += 1
                    print(f"Bradesco +1 linha {i}: padrão transação")
                
                # Indicadores do Banco do Brasil
                if '"DATA","DEPENDENCIA ORIGEM"' in linha_upper:
                    bb_score += 3
                    print(f"BB +3 linha {i}: cabeçalho dependencia")
                
                if '"DATA"' in linha and '"HISTÓRICO"' in linha and '","' in linha:
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
            
            # Se nenhum formato foi claramente identificado, usar heurísticas adicionais
            print("Tentando heurísticas adicionais...")
            
            # Contar separadores para decidir
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
        """Processa CSV do Bradesco"""
        try:
            print("Processando CSV do Bradesco...")
            linhas = csv_string.split('\n')
            print(f"Total de linhas: {len(linhas)}")
            
            # Encontrar a linha que contém todos os dados
            linha_dados = None
            for i, linha in enumerate(linhas):
                if ('Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha) and len(linha) > 100:
                    linha_dados = linha
                    print(f"Linha de dados encontrada na posição {i}")
                    break
            
            if not linha_dados:
                # Se não encontrou em uma linha, tentar encontrar o cabeçalho e pegar dados das próximas linhas
                inicio_dados = -1
                for i, linha in enumerate(linhas):
                    if 'Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha:
                        inicio_dados = i
                        break
                
                if inicio_dados >= 0 and inicio_dados + 1 < len(linhas):
                    # Concatenar as linhas de dados
                    linha_dados = ''.join(linhas[inicio_dados:])
                else:
                    raise Exception("Não foi possível encontrar os dados no CSV do Bradesco")
            
            # Separar cabeçalho dos dados usando \r como separador
            partes = linha_dados.split('\r')
            print(f"Partes encontradas: {len(partes)}")
            
            if len(partes) < 2:
                raise Exception("Formato de dados inválido no CSV do Bradesco")
            
            # O primeiro item deve ser o cabeçalho
            cabecalho = partes[0].strip()
            if not cabecalho.startswith('Data;'):
                # Se não começar com Data, procurar nos primeiros itens
                for parte in partes[:5]:
                    if parte.strip().startswith('Data;'):
                        cabecalho = parte.strip()
                        break
            
            print(f"Cabeçalho identificado: {cabecalho}")
            
            # DEBUG: Mostrar algumas partes para entender o conteúdo
            print("=== PRIMEIRAS 10 PARTES ===")
            for i, parte in enumerate(partes[:10]):
                print(f"Parte {i}: {parte.strip()[:100]}...")
            
            # Filtrar linhas de dados válidas (excluir saldo anterior, totais, etc.)
            linhas_dados = []
            for i, parte in enumerate(partes[1:]):
                linha_limpa = parte.strip()
                if (linha_limpa and 
                    not linha_limpa.startswith('Total;') and 
                    'SALDO ANTERIOR' not in linha_limpa and
                    ';' in linha_limpa and
                    linha_limpa.count(';') >= 4):  # Deve ter pelo menos 5 campos
                    
                    # Verificar se começa com uma data válida (DD/MM/YYYY)
                    if re.match(r'^\d{2}/\d{2}/\d{4};', linha_limpa):
                        linhas_dados.append(linha_limpa)
                        # DEBUG: Mostrar as primeiras 5 linhas válidas
                        if len(linhas_dados) <= 5:
                            print(f"Linha válida {len(linhas_dados)}: {linha_limpa[:100]}...")
            
            print(f"Linhas de dados válidas encontradas: {len(linhas_dados)}")
            
            if len(linhas_dados) == 0:
                raise Exception("Nenhuma linha de dados válida encontrada no CSV do Bradesco")
            
            # Criar CSV estruturado
            csv_estruturado = cabecalho + '\n' + '\n'.join(linhas_dados)
            print(f"CSV estruturado criado com {len(csv_estruturado)} caracteres")
            
            # Ler com pandas
            try:
                df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
                print(f"DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
            except Exception as e:
                print(f"Erro ao criar DataFrame: {e}")
                # Tentar com diferentes configurações
                df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';', encoding='utf-8', on_bad_lines='skip')
                print(f"DataFrame criado (modo alternativo) com {len(df)} linhas")
            
            # Mapear colunas do Bradesco para formato padrão
            colunas_originais = df.columns.tolist()
            print(f"Colunas originais: {colunas_originais}")
            
            # Criar mapeamento flexível
            mapeamento = {}
            for col in colunas_originais:
                col_lower = col.lower().strip()
                if 'data' in col_lower:
                    mapeamento[col] = 'Data'
                elif 'lançamento' in col_lower or 'lancamento' in col_lower:
                    mapeamento[col] = 'Descricao'
                elif 'dcto' in col_lower:
                    mapeamento[col] = 'Documento'
                elif 'crédito' in col_lower or 'credito' in col_lower:
                    mapeamento[col] = 'Credito'
                elif 'débito' in col_lower or 'debito' in col_lower:
                    mapeamento[col] = 'Debito'
                elif 'saldo' in col_lower:
                    mapeamento[col] = 'Saldo'
            
            print(f"Mapeamento de colunas: {mapeamento}")
            
            # Aplicar mapeamento
            df = df.rename(columns=mapeamento)
            
            # Garantir que as colunas essenciais existem
            if 'Credito' not in df.columns:
                df['Credito'] = 0.0
            if 'Debito' not in df.columns:
                df['Debito'] = 0.0
            if 'Documento' not in df.columns:
                df['Documento'] = ''
            
            # Função para processar valores monetários do Bradesco
            def processar_valor_bradesco(valor):
                if pd.isna(valor) or valor == '' or valor is None:
                    return 0.0
                
                # Converter para string e limpar
                valor_str = str(valor).strip()
                if not valor_str or valor_str == 'nan' or valor_str == '':
                    return 0.0
                
                # Remover pontos de milhares e trocar vírgula por ponto
                valor_str = valor_str.replace('.', '').replace(',', '.')
                
                try:
                    resultado = float(valor_str)
                    return abs(resultado)  # SEMPRE retornar valor absoluto
                except Exception as e:
                    print(f"Erro ao processar valor '{valor}': {e}")
                    return 0.0
            
            # Processar valores monetários
            print("Processando valores de crédito...")
            df['Credito'] = df['Credito'].apply(processar_valor_bradesco)
            print("Processando valores de débito...")
            df['Debito'] = df['Debito'].apply(processar_valor_bradesco)
            
            # Debug: mostrar alguns valores processados
            print(f"Primeiros 5 créditos: {df['Credito'].head().tolist()}")
            print(f"Primeiros 5 débitos: {df['Debito'].head().tolist()}")
            
            # CORREÇÃO PRINCIPAL: Lógica correta para Valor e Tipo
            # No Bradesco, se tem valor na coluna Crédito, é entrada (C)
            # Se tem valor na coluna Débito, é saída (D)
            
            # Determinar o tipo baseado em qual coluna tem valor
            df['Tipo'] = df.apply(lambda row: 'C' if row['Credito'] > 0 else 'D', axis=1)
            
            # Para o valor final, usar o que estiver preenchido (crédito OU débito)
            df['Valor'] = df.apply(lambda row: row['Credito'] if row['Credito'] > 0 else row['Debito'], axis=1)
            
            print(f"Valores processados - Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
            print(f"Valores > 0: {(df['Valor'] > 0).sum()}")
            print(f"Valores = 0: {(df['Valor'] == 0).sum()}")
            
            # Debug: mostrar distribuição de valores
            if len(df) > 0:
                print(f"Range de valores: {df['Valor'].min()} até {df['Valor'].max()}")
                print(f"Alguns valores de exemplo: {df['Valor'].head(10).tolist()}")
            
            # Filtrar créditos se necessário
            if not incluir_creditos:
                # Se NÃO incluir créditos, mostrar APENAS débitos
                df_antes = len(df)
                df = df[df['Tipo'] == 'D']
                print(f"Após filtrar créditos (apenas débitos): {len(df)} linhas (eram {df_antes})")
            else:
                # Se incluir créditos, mostrar TUDO (créditos + débitos)
                print(f"Incluindo créditos: mantendo todas as {len(df)} linhas (créditos + débitos)")
                
            # Debug: mostrar valores após filtro
            if len(df) > 0:
                print(f"Valores após filtro: {df['Valor'].head(10).tolist()}")
                print(f"Tipos após filtro - Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
            
            # Limpar dados - CORRIGIDO
            df = df.dropna(subset=['Descricao'])
            print(f"Após remover descrições vazias: {len(df)} linhas")
            
            # Filtrar apenas valores válidos (> 0)
            df = df[df['Valor'] > 0]
            print(f"Após filtrar valores > 0: {len(df)} linhas")
            
            # Se ainda não temos dados, mostrar debug detalhado
            if len(df) == 0:
                print("ERRO: Nenhuma linha válida encontrada após processamento!")
                print("Verificando dados originais...")
                
                # Recarregar para debug
                df_debug = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
                df_debug = df_debug.rename(columns=mapeamento)
                
                print(f"Dados originais - primeiras 5 linhas:")
                for i, row in df_debug.head().iterrows():
                    print(f"  Linha {i}: Credito='{row.get('Credito', 'N/A')}', Debito='{row.get('Debito', 'N/A')}', Descricao='{row.get('Descricao', 'N/A')}'")
                
                return pd.DataFrame(columns=['Data', 'Descricao', 'Valor', 'Tipo', 'Documento'])
            
            # Processar datas
            try:
                df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
            except Exception as e:
                print(f"Aviso: erro ao processar datas: {e}")
                # Manter como string se não conseguir converter
            
            # Retornar apenas as colunas necessárias
            colunas_resultado = ['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']
            resultado = df[colunas_resultado].reset_index(drop=True)
            
            print(f"Resultado final: {len(resultado)} linhas")
            if len(resultado) > 0:
                print(f"Amostra dos dados: {resultado.head(3).to_dict()}")
            
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
            
            # Aplicar filtro de créditos
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']  # Apenas débitos
            # Se incluir_creditos = True, manter tudo (não filtrar)
                
        elif 'Historico' in df.columns:
            # Formato novo
            df = df.dropna(subset=['Historico', 'Valor'])
            df = df[df['Historico'] != 'Saldo Anterior']
            
            df['Descricao'] = df['Historico']
            df['Agencia'] = df.get('Dependencia Origem', '')
            df['Documento'] = df.get('Numero do documento', '')
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
            
            # Aplicar filtro de créditos
            if not incluir_creditos:
                df = df[df['Tipo'] == 'D']  # Apenas débitos
            # Se incluir_creditos = True, manter tudo (não filtrar)
        else:
            raise Exception("Formato de CSV do Banco do Brasil não reconhecido")
        
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        df = df.dropna(subset=['Valor'])
        
        print(f"Banco do Brasil - Total final: {len(df)} linhas")
        print(f"Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
        
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