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
        
        valor_str = str(valor).strip().replace('"', '')
        if not valor_str or valor_str.lower() == 'nan':
            return 0.0
        
        # Tratar valores negativos
        negativo = valor_str.startswith('-')
        if negativo:
            valor_str = valor_str[1:]
        
        # Formato brasileiro: remover pontos (milhares) e trocar vírgula por ponto
        valor_str = valor_str.replace('.', '').replace(',', '.')
        
        try:
            resultado = float(valor_str)
            return -resultado if negativo else resultado
        except:
            return 0.0

    # ==========================================
    # PROCESSAMENTO EXCEL
    # ==========================================
    
    def processar_excel(self, excel_data):
        """Processa arquivo Excel de categorias"""
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
            
            print(f"Categorias carregadas: {len(categorias)}")
            return categorias
            
        except Exception as e:
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
        linhas = csv_string.split('\n')[:10]
        
        score_novo = 0
        score_antigo = 0
        
        for linha in linhas:
            # Formato NOVO: cabeçalho específico e linhas organizadas
            if 'Data;Histórico;Docto.' in linha or 'Data;Historico;Docto.' in linha:
                score_novo += 5
            
            # Formato NOVO: transações em linhas separadas, poucos \r
            if re.match(r'^\d{2}/\d{2}/\d{2,4};', linha) and linha.count('\r') <= 1:
                score_novo += 2
            
            # Formato ANTIGO: dados aglomerados com muitos \r
            if linha.count('\r') > 10 and ';' in linha:
                score_antigo += 3
            
            # Formato ANTIGO: cabeçalho específico
            if 'Data;Lançamento;Dcto.' in linha:
                score_antigo += 3
            
            # Formato ANTIGO: muitos campos na mesma linha
            if linha.count(';') > 20:
                score_antigo += 2
        
        print(f"Formato Bradesco - Novo: {score_novo}, Antigo: {score_antigo}")
        return 'novo' if score_novo > score_antigo else 'antigo'

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
            
            # Detectar formato e processar
            formato = self.detectar_formato_csv(csv_string)
            print(f"Formato detectado: {formato}")
            
            if formato == 'bradesco':
                return self.processar_csv_bradesco(csv_string)
            elif formato == 'banco_brasil':
                return self.processar_csv_banco_brasil(csv_string)
            else:
                raise Exception(f"Formato não reconhecido: {formato}")
                
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
        print("=== PROCESSANDO BRADESCO NOVO ===")
        
        linhas = csv_string.split('\n')
        
        # Encontrar cabeçalho
        header_line = -1
        for i, linha in enumerate(linhas):
            if 'Data;Histórico;Docto.' in linha or 'Data;Historico;Docto.' in linha:
                header_line = i
                break
        
        if header_line == -1:
            raise Exception("Cabeçalho não encontrado no formato novo")
        
        # Extrair dados
        cabecalho = linhas[header_line].strip()
        linhas_dados = []
        
        for i in range(header_line + 1, len(linhas)):
            linha = linhas[i].strip()
            
            # Filtrar linhas válidas
            if (linha and 
                not any(x in linha.upper() for x in ['SALDO ANTERIOR', 'TOTAL;', 'OS DADOS ACIMA', 'ÚLTIMOS LANÇAMENTOS']) and
                re.match(r'^\d{2}/\d{2}/\d{2,4};', linha) and
                linha.count(';') >= 4):
                linhas_dados.append(linha)
        
        print(f"Encontradas {len(linhas_dados)} linhas de dados")
        
        if not linhas_dados:
            raise Exception("Nenhuma linha válida encontrada")
        
        # Criar DataFrame
        csv_estruturado = cabecalho + '\n' + '\n'.join(linhas_dados)
        df = pd.read_csv(io.StringIO(csv_estruturado), delimiter=';')
        
        # Mapear colunas
        df = self.mapear_colunas_bradesco_novo(df)
        
        # Processar valores
        df = self.processar_valores_bradesco_novo(df)
        
        # Limpar dados
        df = df[df['Valor'] > 0].dropna(subset=['Descricao'])
        
        # Processar datas
        df = self.processar_datas_bradesco(df)
        
        resultado = df[['Data', 'Descricao', 'Valor', 'Tipo', 'Documento']].reset_index(drop=True)
        print(f"Resultado final: {len(resultado)} linhas")
        
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
        df['Credito'] = df['Credito'].apply(self.processar_valor_monetario)
        df['Debito'] = df['Debito'].apply(self.processar_valor_monetario)
        
        # Determinar tipo e valor
        def determinar_tipo_valor(row):
            if row['Credito'] > 0:
                return 'C', row['Credito']
            elif row['Debito'] != 0:
                return 'D', abs(row['Debito'])
            else:
                return 'D', 0.0
        
        df[['Tipo', 'Valor']] = df.apply(lambda row: pd.Series(determinar_tipo_valor(row)), axis=1)
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