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
            print("  Primeiras linhas:")
            for i in range(min(5, len(df))):
                print(f"    {i}: Grupo='{df.iloc[i]['Grupo']}', Palavra='{df.iloc[i]['Palavra_Chave']}'")
            
            # Processar categorias
            categorias = {}
            categoria_atual = None
            
            for index, row in df.iterrows():
                # Se tem grupo definido, usar como categoria atual
                if pd.notna(row['Grupo']) and str(row['Grupo']).strip():
                    categoria_atual = str(row['Grupo']).strip()
                    print(f"Nova categoria: '{categoria_atual}'")
                
                # Se tem palavra-chave e categoria atual, adicionar
                if pd.notna(row['Palavra_Chave']) and categoria_atual:
                    palavra = str(row['Palavra_Chave']).strip()
                    if palavra:  # Só adicionar se não estiver vazio
                        categorias[palavra] = categoria_atual
                        print(f"  Palavra-chave: '{palavra}' -> '{categoria_atual}'")
            
            print(f"Total de categorias processadas: {len(categorias)}")
            
            if len(categorias) == 0:
                raise Exception("Nenhuma categoria válida encontrada no Excel. Verifique o formato do arquivo.")
            
            return categorias
            
        except Exception as e:
            print(f"Erro detalhado no Excel: {e}")
            raise Exception(f"Erro no Excel: {e}")

    # ==========================================
    # DETECÇÃO DE FORMATO CSV
    # ==========================================
    
    def detectar_formato_csv(self, csv_string):
        """Detecta formato: Banco do Brasil, Bradesco Novo ou Bradesco Antigo"""
        print("=== DETECTANDO FORMATO CSV ===")
        
        linhas = csv_string.split('\n')[:15]  # Analisar apenas primeiras linhas
        
        # Scores para cada formato
        bb_score = 0
        bradesco_score = 0
        
        for i, linha in enumerate(linhas):
            linha_upper = linha.upper()
            
            # Indicadores Banco do Brasil
            if '"DATA","DEPENDENCIA ORIGEM"' in linha_upper:
                bb_score += 5
            if '"DATA"' in linha and '"HISTÓRICO"' in linha and '","' in linha:
                bb_score += 3
            if linha.count('","') > 3 and linha.startswith('"'):
                bb_score += 1
            
            # Indicadores Bradesco (ambos formatos)
            if 'EXTRATO DE:' in linha_upper and ('AGÊNCIA:' in linha_upper or 'CONTA:' in linha_upper):
                bradesco_score += 5
            if 'DATA;' in linha_upper and ('HISTÓRICO' in linha_upper or 'LANÇAMENTO' in linha_upper):
                bradesco_score += 3
            if linha.count(';') > linha.count(',') and ';' in linha:
                bradesco_score += 1
        
        print(f"Scores - Bradesco: {bradesco_score}, BB: {bb_score}")
        
        if bradesco_score > bb_score:
            return 'bradesco'
        elif bb_score > 0:
            return 'banco_brasil'
        else:
            return 'desconhecido'

    def detectar_formato_bradesco(self, csv_string):
        """Detecta se é Bradesco NOVO ou ANTIGO"""
        print("=== DETECTANDO FORMATO BRADESCO (NOVO vs ANTIGO) ===")
        
        linhas = csv_string.split('\n')[:15]  # Analisar mais linhas
        
        score_novo = 0
        score_antigo = 0
        
        # Debug: mostrar primeiras linhas
        print("Primeiras linhas para análise:")
        for i, linha in enumerate(linhas[:5]):
            print(f"  {i}: {linha[:100]}...")
        
        for i, linha in enumerate(linhas):
            linha_upper = linha.upper()
            
            # ===== INDICADORES FORMATO NOVO =====
            
            # Cabeçalho específico do novo
            if 'DATA;HISTÓRICO;DOCTO.' in linha_upper or 'DATA;HISTORICO;DOCTO.' in linha_upper:
                score_novo += 10
                print(f"NOVO +10 linha {i}: cabeçalho novo detectado")
            
            # Extrato header do novo (mais específico)
            if 'EXTRATO DE: AG:' in linha_upper and 'CONTA:' in linha_upper and 'ENTRE' in linha_upper:
                score_novo += 5
                print(f"NOVO +5 linha {i}: header novo detectado")
            
            # Transações organizadas (formato novo)
            if re.match(r'^\d{2}/\d{2}/\d{2,4};[^;]+;\d*;', linha):
                if linha.count('\r') <= 1:  # Poucos \r
                    score_novo += 3
                    print(f"NOVO +3 linha {i}: transação organizada")
            
            # Seção "Últimos Lançamentos" (específico do novo)
            if 'ÚLTIMOS LANÇAMENTOS' in linha_upper:
                score_novo += 3
                print(f"NOVO +3 linha {i}: seção últimos lançamentos")
            
            # ===== INDICADORES FORMATO ANTIGO =====
            
            # Cabeçalho específico do antigo
            if 'DATA;LANÇAMENTO;DCTO.' in linha_upper or 'DATA;LANCAMENTO;DCTO.' in linha_upper:
                score_antigo += 8
                print(f"ANTIGO +8 linha {i}: cabeçalho antigo detectado")
            
            # Dados aglomerados com muitos \r (característico do antigo)
            if linha.count('\r') > 15 and ';' in linha:
                score_antigo += 5
                print(f"ANTIGO +5 linha {i}: muitos \\r ({linha.count('\\r')})")
            
            # Uma linha muito longa com muitos campos (formato antigo)
            if linha.count(';') > 30:
                score_antigo += 4
                print(f"ANTIGO +4 linha {i}: muitos campos ({linha.count(';')})")
            
            # Header de agência diferente (formato antigo)
            if 'AGÊNCIA:' in linha_upper and 'CONTA:' in linha_upper and 'EXTRATO DE:' in linha_upper:
                if 'AG:' not in linha_upper:  # Diferente do novo
                    score_antigo += 3
                    print(f"ANTIGO +3 linha {i}: header antigo")
        
        print(f"SCORES FINAIS - Novo: {score_novo}, Antigo: {score_antigo}")
        
        # Critério mais rigoroso para novo
        if score_novo >= score_antigo and score_novo >= 5:
            print("FORMATO BRADESCO NOVO DETECTADO")
            return 'novo'
        else:
            print("FORMATO BRADESCO ANTIGO DETECTADO")
            return 'antigo'

    # ==========================================
    # PROCESSAMENTO CSV
    # ==========================================
    
    def processar_csv(self, csv_data):
        """Processa CSV - ponto de entrada principal"""
        try:
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
            
            # Detectar formato principal (BB vs Bradesco)
            formato = self.detectar_formato_csv(csv_string)
            print(f"Formato detectado: {formato}")
            
            if formato == 'bradesco':
                # Usar diretamente o processador universal (sem sub-detecção)
                return self.processar_csv_bradesco(csv_string)
            elif formato == 'banco_brasil':
                return self.processar_csv_banco_brasil(csv_string)
            else:
                # Se não conseguiu detectar, tentar Bradesco mesmo assim
                print("Formato não reconhecido, tentando como Bradesco...")
                return self.processar_csv_bradesco(csv_string)
                
        except Exception as e:
            print(f"Erro no processamento CSV: {e}")
            raise e

    def processar_csv_bradesco(self, csv_string):
        """Processa CSV Bradesco - detecta se é novo ou antigo"""
        formato_bradesco = self.detectar_formato_bradesco(csv_string)
        
        if formato_bradesco == 'novo':
            return self.processar_bradesco_novo(csv_string)
        else:
            return self.processar_bradesco_antigo(csv_string)

    def processar_bradesco_novo(self, csv_string):
        """Processa formato NOVO do Bradesco"""
        print("=== PROCESSANDO BRADESCO FORMATO NOVO ===")
        
        linhas = csv_string.split('\n')
        print(f"Total de linhas no arquivo: {len(linhas)}")
        
        # Debug: mostrar estrutura do arquivo
        print("Estrutura do arquivo (primeiras 15 linhas):")
        for i, linha in enumerate(linhas[:15]):
            print(f"  {i}: {linha[:100]}...")
        
        # Encontrar cabeçalho - ser MUITO mais flexível
        header_line = -1
        cabecalho_encontrado = None
        
        # Procurar por diferentes padrões de cabeçalho
        padroes_cabecalho = [
            'Data;Histórico;Docto.',
            'Data;Historico;Docto.',
            'Data;Histórico;Docto;',
            'Data;Historico;Docto;',
            'Data;Lançamento;Dcto.',
            'Data;Lancamento;Dcto.',
            'Data;Histórico;Documento',
            'Data;Historico;Documento'
        ]
        
        for i, linha in enumerate(linhas):
            for padrao in padroes_cabecalho:
                if padrao in linha:
                    header_line = i
                    cabecalho_encontrado = linha.strip()
                    print(f"Cabeçalho encontrado na linha {i} com padrão '{padrao}': {cabecalho_encontrado}")
                    break
            if header_line != -1:
                break
        
        # Se não encontrou com padrões específicos, procurar qualquer linha com "Data;" e múltiplos ";"
        if header_line == -1:
            print("Tentando busca mais ampla por cabeçalho...")
            for i, linha in enumerate(linhas):
                if ('Data;' in linha and 
                    linha.count(';') >= 4 and 
                    any(palavra in linha.upper() for palavra in ['HISTÓRICO', 'HISTORICO', 'LANÇAMENTO', 'LANCAMENTO'])):
                    header_line = i
                    cabecalho_encontrado = linha.strip()
                    print(f"Cabeçalho alternativo encontrado na linha {i}: {linha[:100]}...")
                    break
        
        if header_line == -1:
            print("ERRO: Cabeçalho não encontrado!")
            print("Tentando análise manual das primeiras 20 linhas:")
            for i, linha in enumerate(linhas[:20]):
                if linha.strip():
                    campos = linha.count(';')
                    tem_data = bool(re.search(r'\d{2}/\d{2}/\d{2,4}', linha))
                    print(f"  Linha {i}: campos={campos}, tem_data={tem_data}, conteúdo: {linha[:80]}...")
            
            raise Exception("Cabeçalho não encontrado no formato novo. Verifique se o arquivo está correto.")
        
        # Extrair linhas de dados após o cabeçalho
        linhas_dados = []
        secao_atual = "principal"
        
        print(f"Processando linhas a partir da linha {header_line + 1}...")
        
        # Processar TODAS as linhas após o cabeçalho, sendo mais permissivo
        for i in range(header_line + 1, len(linhas)):
            linha_original = linhas[i]
            linha = linha_original.strip()
            
            if not linha:  # Pular linhas vazias
                continue
            
            # Debug das primeiras 10 linhas processadas
            if i - header_line <= 10:
                print(f"  Analisando linha {i}: '{linha[:80]}...'")
                print(f"    Campos: {linha.count(';')}")
                print(f"    Match data: {bool(re.match(r'^\\d{2}/\\d{2}/\\d{2,4};', linha))}")
            
            # Detectar seções especiais (mas não parar o processamento)
            if any(secao in linha.upper() for secao in ['ÚLTIMOS LANÇAMENTOS', 'ULTIMOS LANCAMENTOS']):
                print(f"Seção 'Últimos Lançamentos' detectada na linha {i}")
                secao_atual = "ultimos"
                continue
            
            # Parar apenas em indicadores claros de fim
            if any(fim in linha.upper() for fim in ['OS DADOS ACIMA', 'TOTAL GERAL', 'GERADO EM']):
                print(f"Fim dos dados detectado na linha {i}: {linha[:50]}...")
                break
            
            # Filtrar linhas que claramente não são dados
            if any(excluir in linha.upper() for excluir in [
                'SALDO ANTERIOR', 
                'EXTRATO DE:',
                'AGÊNCIA:',
                'CONTA:',
                'DATA;HISTÓRICO;VALOR',  # Cabeçalho de seção especial
                'DATA;HISTORICO;VALOR'
            ]):
                print(f"Linha ignorada (controle): {linha[:50]}...")
                continue
            
            # Critérios MAIS FLEXÍVEIS para linha de dados válida
            eh_linha_valida = False
            
            # Critério 1: Começa com data no formato DD/MM/YY ou DD/MM/YYYY
            if re.match(r'^\d{2}/\d{2}/\d{2,4};', linha):
                eh_linha_valida = True
                motivo = "formato de data"
            
            # Critério 2: Tem pelo menos 3 campos e contém uma data em algum lugar
            elif linha.count(';') >= 2 and re.search(r'\d{2}/\d{2}/\d{2,4}', linha):
                eh_linha_valida = True
                motivo = "contém data"
            
            # Critério 3: Linha com muitos campos numéricos (possível transação)
            elif (linha.count(';') >= 4 and 
                  re.search(r'\d+[,\.]\d+', linha) and  # Tem valores monetários
                  not linha.upper().startswith('TOTAL')):
                eh_linha_valida = True
                motivo = "valores monetários"
            
            if eh_linha_valida:
                linhas_dados.append(linha)
                
                # Debug: mostrar primeiras 5 linhas válidas
                if len(linhas_dados) <= 5:
                    print(f"✓ Linha dados {len(linhas_dados)} ({motivo}): {linha[:80]}...")
            else:
                # Debug: mostrar por que foi rejeitada (só para as primeiras)
                if i - header_line <= 15:
                    print(f"✗ Linha rejeitada: {linha[:50]}... (campos: {linha.count(';')})")
        
        print(f"Total de linhas de dados encontradas: {len(linhas_dados)}")
        
        if not linhas_dados:
            print("ERRO: Nenhuma linha de dados encontrada!")
            print("Fazendo análise detalhada das linhas após o cabeçalho:")
            
            inicio_analise = max(0, header_line + 1)
            fim_analise = min(len(linhas), header_line + 30)
            
            for i in range(inicio_analise, fim_analise):
                linha = linhas[i].strip()
                if linha:
                    print(f"  Linha {i}:")
                    print(f"    Conteúdo: '{linha}'")
                    print(f"    Campos (;): {linha.count(';')}")
                    print(f"    Match data: {bool(re.match(r'^\\d{2}/\\d{2}/\\d{2,4};', linha))}")
                    print(f"    Contém data: {bool(re.search(r'\\d{2}/\\d{2}/\\d{2,4}', linha))}")
                    print(f"    Contém valores: {bool(re.search(r'\\d+[,\\.\\d]*', linha))}")
                    print()
            
            raise Exception("Nenhuma linha de dados válida encontrada no formato novo")
        
        # Criar CSV estruturado
        csv_estruturado = cabecalho_encontrado + '\n' + '\n'.join(linhas_dados)
        print(f"CSV estruturado criado com {len(csv_estruturado)} caracteres")
        
        # Criar DataFrame
        try:
            df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
            print(f"DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
        except Exception as e:
            print(f"Erro ao criar DataFrame: {e}")
            print("Conteúdo do CSV estruturado (primeiros 1000 chars):")
            print(csv_estruturado[:1000])
            print("...")
            print("Tentando com configurações alternativas...")
            
            try:
                # Tentar com outras configurações
                df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';', on_bad_lines='skip')
                print(f"DataFrame criado com modo alternativo: {len(df)} linhas")
            except Exception as e2:
                print(f"Erro mesmo com modo alternativo: {e2}")
                raise e
        
        # Mapear colunas
        df = self.mapear_colunas_bradesco_novo(df)
        print(f"Colunas após mapeamento: {list(df.columns)}")
        
        # Processar valores
        df = self.processar_valores_bradesco_novo(df)
        print(f"Valores processados - Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
        
        # Limpar dados
        df_antes = len(df)
        df = df[df['Valor'] > 0].dropna(subset=['Descricao'])
        print(f"Após limpeza: {len(df)} linhas (eram {df_antes})")
        
        if len(df) == 0:
            print("ERRO: Nenhuma linha válida após processamento!")
            # Debug mais detalhado
            df_debug = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
            df_debug = self.mapear_colunas_bradesco_novo(df_debug)
            print("Análise detalhada dos dados:")
            print("Primeiras 5 linhas antes do processamento de valores:")
            for i in range(min(5, len(df_debug))):
                row = df_debug.iloc[i]
                print(f"  Linha {i}:")
                for col in df_debug.columns:
                    print(f"    {col}: '{row[col]}'")
                print()
            
            raise Exception("Nenhuma transação válida encontrada após processamento dos valores")
        
        # Processar datas
        df = self.processar_datas_bradesco(df)
        
        # Retornar resultado
        resultado = df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].reset_index(drop=True)
        print(f"Resultado final formato novo: {len(resultado)} linhas")
        
        # Debug final
        if len(resultado) > 0:
            print("Amostra do resultado:")
            for i, row in resultado.head(3).iterrows():
                print(f"  {i}: Data={row['Data']}, Desc='{row['Descricao'][:30]}...', Valor={row['Valor']}, Tipo={row['Tipo']}")
        
        return resultado

    def processar_bradesco_antigo(self, csv_string):
        """Processa formato ANTIGO do Bradesco"""
        print("=== PROCESSANDO BRADESCO ANTIGO ===")
        
        linhas = csv_string.split('\n')
        
        # Encontrar linha com dados (formato antigo tem tudo em uma linha longa)
        linha_dados = None
        for linha in linhas:
            if ('Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha) and len(linha) > 100:
                linha_dados = linha
                break
        
        if not linha_dados:
            # Tentar concatenar linhas se não encontrou em uma só
            for i, linha in enumerate(linhas):
                if 'Data;Lançamento;Dcto.' in linha or 'Data;Lan' in linha:
                    linha_dados = ''.join(linhas[i:])
                    break
        
        if not linha_dados:
            raise Exception("Dados não encontrados no formato antigo")
        
        # Separar usando \r como delimitador
        partes = linha_dados.split('\r')
        
        # Primeiro item é o cabeçalho
        cabecalho = partes[0].strip()
        if not cabecalho.startswith('Data;'):
            # Procurar cabeçalho nas primeiras partes
            for parte in partes[:5]:
                if parte.strip().startswith('Data;'):
                    cabecalho = parte.strip()
                    break
        
        # Filtrar linhas de dados válidas
        linhas_dados = []
        for parte in partes[1:]:
            linha_limpa = parte.strip()
            if (linha_limpa and 
                not any(x in linha_limpa.upper() for x in ['TOTAL;', 'SALDO ANTERIOR']) and
                ';' in linha_limpa and
                linha_limpa.count(';') >= 4 and
                re.match(r'^\d{2}/\d{2}/\d{4};', linha_limpa)):
                linhas_dados.append(linha_limpa)
        
        print(f"Encontradas {len(linhas_dados)} linhas de dados")
        
        if not linhas_dados:
            raise Exception("Nenhuma linha válida encontrada no formato antigo")
        
        # Criar DataFrame
        csv_estruturado = cabecalho + '\n' + '\n'.join(linhas_dados)
        df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
        
        # Mapear colunas
        df = self.mapear_colunas_bradesco_antigo(df)
        
        # Processar valores
        df = self.processar_valores_bradesco_antigo(df)
        
        # Limpar dados
        df = df[df['Valor'] > 0].dropna(subset=['Descricao'])
        
        # Processar datas
        df = self.processar_datas_bradesco(df)
        
        resultado = df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].reset_index(drop=True)
        print(f"Resultado final: {len(resultado)} linhas")
        
        return resultado

    def processar_csv_banco_brasil(self, csv_string):
        """Processa CSV do Banco do Brasil"""
        print("=== PROCESSANDO BANCO DO BRASIL ===")
        
        # Limpar caracteres problemáticos
        csv_string = csv_string.replace('Histórico', 'Historico').replace('Número', 'Numero')
        
        df = pd.read_csv(io.StringIO(csv_string))
        
        # Detectar formato
        if 'Descrição' in df.columns or 'Descricao' in df.columns:
            # Formato antigo
            desc_col = 'Descrição' if 'Descrição' in df.columns else 'Descricao'
            df = df.dropna(subset=[desc_col, 'Valor'])
            df['Descricao'] = df[desc_col]
            df['Agencia'] = df.get('Agência', df.get('Agencia', ''))
            df['Documento'] = df.get('Documento', '')
        elif 'Historico' in df.columns:
            # Formato novo
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
        
        print(f"Banco do Brasil processado: {len(df)} linhas")
        return df

    # ==========================================
    # HELPERS ESPECÍFICOS BRADESCO
    # ==========================================
    
    def mapear_colunas_bradesco_novo(self, df):
        """Mapeia colunas do formato novo do Bradesco"""
        mapeamento = {
            'Data': 'Data',
            'Histórico': 'Descricao',
            'Historico': 'Descricao',
            'Docto.': 'Documento',
            'Crédito (R$)': 'Credito',
            'Débito (R$)': 'Debito',
            'Saldo (R$)': 'Saldo'
        }
        
        # Aplicar mapeamento flexível
        for col_original in df.columns:
            for key, value in mapeamento.items():
                if key.lower() in col_original.lower():
                    df = df.rename(columns={col_original: value})
                    break
        
        # Garantir colunas essenciais
        for col in ['Credito', 'Debito', 'Documento']:
            if col not in df.columns:
                df[col] = '' if col == 'Documento' else 0.0
        
        return df

    def mapear_colunas_bradesco_antigo(self, df):
        """Mapeia colunas do formato antigo do Bradesco"""
        mapeamento = {}
        for col in df.columns:
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
        
        df = df.rename(columns=mapeamento)
        
        # Garantir colunas essenciais
        for col in ['Credito', 'Debito', 'Documento']:
            if col not in df.columns:
                df[col] = '' if col == 'Documento' else 0.0
        
        return df

    def processar_valores_bradesco_novo(self, df):
        """Processa valores do formato novo"""
        print("=== PROCESSANDO VALORES FORMATO NOVO ===")
        
        # Debug: mostrar estrutura das colunas
        print(f"Colunas disponíveis: {list(df.columns)}")
        
        # Garantir que as colunas existem
        if 'Credito' not in df.columns:
            df['Credito'] = ''
        if 'Debito' not in df.columns:
            df['Debito'] = ''
        
        # Debug: mostrar alguns valores brutos
        print("Primeiros valores brutos:")
        for i in range(min(3, len(df))):
            print(f"  Linha {i}: Credito='{df.iloc[i]['Credito']}', Debito='{df.iloc[i]['Debito']}'")
        
        # Processar valores monetários
        df['Credito'] = df['Credito'].apply(self.processar_valor_monetario)
        df['Debito'] = df['Debito'].apply(self.processar_valor_monetario)
        
        print(f"Após processamento:")
        print(f"  Créditos > 0: {(df['Credito'] > 0).sum()}")
        print(f"  Débitos != 0: {(df['Debito'] != 0).sum()}")
        
        # Determinar tipo e valor - LÓGICA CORRIGIDA
        def determinar_tipo_valor(row):
            credito = row['Credito']
            debito = row['Debito']
            
            # Debug para primeiras linhas
            if row.name < 3:
                print(f"  Linha {row.name}: Credito={credito}, Debito={debito}")
            
            # Se tem crédito > 0, é entrada (C)
            if credito > 0:
                return 'C', credito
            # Se tem débito != 0, é saída (D)
            elif debito != 0:
                return 'D', abs(debito)  # Sempre positivo
            # Se ambos são zero, pode ser linha de saldo ou inválida
            else:
                return 'D', 0.0
        
        # Aplicar lógica
        df[['Tipo', 'Valor']] = df.apply(lambda row: pd.Series(determinar_tipo_valor(row)), axis=1)
        
        print(f"Resultado final:")
        print(f"  Tipos C (Crédito): {(df['Tipo'] == 'C').sum()}")
        print(f"  Tipos D (Débito): {(df['Tipo'] == 'D').sum()}")
        print(f"  Valores > 0: {(df['Valor'] > 0).sum()}")
        print(f"  Valores = 0: {(df['Valor'] == 0).sum()}")
        
        return df

    def processar_valores_bradesco_antigo(self, df):
        """Processa valores do formato antigo"""
        df['Credito'] = df['Credito'].apply(lambda x: abs(self.processar_valor_monetario(x)))
        df['Debito'] = df['Debito'].apply(lambda x: abs(self.processar_valor_monetario(x)))
        
        # No formato antigo: se tem crédito > 0, é C; senão é D
        df['Tipo'] = df.apply(lambda row: 'C' if row['Credito'] > 0 else 'D', axis=1)
        df['Valor'] = df.apply(lambda row: row['Credito'] if row['Credito'] > 0 else row['Debito'], axis=1)
        
        return df

    def processar_datas_bradesco(self, df):
        """Processa datas do Bradesco (novo e antigo)"""
        try:
            # Tentar formato DD/MM/YY primeiro
            df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%y', errors='coerce')
            
            # Se não funcionou, tentar DD/MM/YYYY
            if df['Data'].isna().all():
                df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
        except:
            print("Mantendo datas como string")
        
        return df

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
            print(f"{nome_tipo}: {len(resultados)} categorias")
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
            
            # Resumo por categoria - GERAL
            ws_resumo.append(["RESUMO GERAL POR CATEGORIA"])
            ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws_resumo.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # === ABA RESUMO CRÉDITOS ===
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
            
            # === ABA RESUMO DÉBITOS ===
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
            
            # === FUNÇÃO PARA CRIAR ABAS DETALHADAS ===
            def criar_aba_categoria(resultado, prefixo=""):
                categoria = resultado['categoria']
                
                # Nome da aba (máximo 31 caracteres, sem caracteres especiais)
                nome_aba = f"{prefixo}{categoria}".replace('/', '-').replace('\\', '-').replace('*', '-')
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
            
            # Criar abas para categorias gerais
            for resultado in categorias_gerais:
                criar_aba_categoria(resultado)
            
            # Criar abas para créditos (se houver)
            for resultado in categorias_creditos:
                criar_aba_categoria(resultado, "C_")
            
            # Criar abas para débitos (se houver)
            for resultado in categorias_debitos:
                criar_aba_categoria(resultado, "D_")
            
            # Ajustar largura das colunas dos resumos
            ws_resumo.column_dimensions['A'].width = 25
            ws_resumo.column_dimensions['B'].width = 15
            ws_resumo.column_dimensions['C'].width = 12
            ws_resumo.column_dimensions['D'].width = 12
            
            if len(categorias_creditos) > 0:
                ws_creditos.column_dimensions['A'].width = 25
                ws_creditos.column_dimensions['B'].width = 15
                ws_creditos.column_dimensions['C'].width = 12
                ws_creditos.column_dimensions['D'].width = 12
            
            if len(categorias_debitos) > 0:
                ws_debitos.column_dimensions['A'].width = 25
                ws_debitos.column_dimensions['B'].width = 15
                ws_debitos.column_dimensions['C'].width = 12
                ws_debitos.column_dimensions['D'].width = 12
            
            # Salvar Excel
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None
    
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