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
            # Verificar se é endpoint de procedimentos
            if self.path == '/api/procedures':
                return self.processar_procedimentos()
            
            # Código original para extratos
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
            df = self.processar_csv(csv_data, incluir_creditos=True)
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
    
    def processar_procedimentos(self):
        """Processa dados de procedimentos médicos"""
        try:
            print("=== PROCESSANDO PROCEDIMENTOS MÉDICOS ===")
            
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            content_type = self.headers.get('Content-Type', '')
            boundary = content_type.split('boundary=')[1]
            
            files, form_data = self.parse_multipart(post_data, boundary)
            procedures_data = files.get('procedures_file')
            categories_data = files.get('categories_file')
            
            if not procedures_data or not categories_data:
                raise Exception("Ambos os arquivos (procedimentos e categorias) são necessários")
            
            # Processar arquivos
            categorias = self.processar_arquivo_categorias(categories_data)
            df_procedimentos = self.processar_arquivo_procedimentos(procedures_data)
            
            # Analisar procedimentos
            analise = self.analisar_procedimentos_medicos(df_procedimentos, categorias)
            
            # Gerar Excel
            excel_b64 = self.gerar_excel_procedimentos(analise, df_procedimentos)
            
            resposta = {
                'success': True,
                'estatisticas': analise['estatisticas'],
                'categorias': analise['categorias'],
                'procedimentos': analise['procedimentos'],
                'unidades': analise['unidades'],
                'excel_file': excel_b64
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(resposta).encode())
            
        except Exception as e:
            print(f"Erro nos procedimentos: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {'success': False, 'error': str(e)}
            self.wfile.write(json.dumps(error_response).encode())

    def processar_arquivo_categorias(self, categories_data):
        """Processa arquivo Excel de categorias"""
        try:
            df = pd.read_excel(io.BytesIO(categories_data))
            
            # Extrair lista de categorias da primeira coluna
            categorias = []
            for _, row in df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip():
                    categorias.append(str(row.iloc[0]).strip())
            
            print(f"Categorias carregadas: {categorias}")
            return categorias
            
        except Exception as e:
            raise Exception(f"Erro ao processar arquivo de categorias: {e}")

    def processar_arquivo_procedimentos(self, procedures_data):
        """Processa arquivo Excel de procedimentos"""
        try:
            df = pd.read_excel(io.BytesIO(procedures_data))
            
            print(f"Colunas disponíveis: {df.columns.tolist()}")
            
            # Verificar se tem as colunas necessárias
            colunas_necessarias = ['Unidade', 'Procedimento', 'Total do Item']
            colunas_encontradas = []
            
            for col_necessaria in colunas_necessarias:
                col_encontrada = None
                for col_original in df.columns:
                    if col_necessaria.lower() in col_original.lower():
                        col_encontrada = col_original
                        break
                
                if col_encontrada:
                    colunas_encontradas.append(col_encontrada)
                else:
                    raise Exception(f"Coluna '{col_necessaria}' não encontrada no arquivo")
            
            # Renomear colunas para padrão
            df_normalizado = df[colunas_encontradas].copy()
            df_normalizado.columns = ['Unidade', 'Procedimento', 'TotalItem']
            
            # Limpar dados
            df_normalizado = df_normalizado.dropna(subset=['Procedimento', 'TotalItem'])
            df_normalizado['TotalItem'] = pd.to_numeric(df_normalizado['TotalItem'], errors='coerce')
            df_normalizado = df_normalizado.dropna(subset=['TotalItem'])
            df_normalizado = df_normalizado[df_normalizado['TotalItem'] > 0]
            
            # Garantir que Unidade não seja nula
            df_normalizado['Unidade'] = df_normalizado['Unidade'].fillna('Não informado')
            
            print(f"Registros processados: {len(df_normalizado)}")
            return df_normalizado
            
        except Exception as e:
            raise Exception(f"Erro ao processar arquivo de procedimentos: {e}")

    def mapear_procedimento_para_categoria(self, procedimento, categorias):
        """Mapeia procedimento para categoria usando palavras-chave"""
        if not procedimento:
            return "Outros"
        
        proc_upper = str(procedimento).upper()
        
        # Buscar correspondência com categorias
        for categoria in categorias:
            cat_upper = str(categoria).upper().strip()
            
            # Verificar se a categoria está contida no procedimento
            if cat_upper in proc_upper:
                return categoria
            
            # Verificações específicas adicionais
            if cat_upper == 'VITAMINA' and ('VITAMINA' in proc_upper or 'B12' in proc_upper):
                return categoria
            if cat_upper == 'CONSULTAS' and ('CONSULTA' in proc_upper):
                return categoria
        
        return "Outros"

    def analisar_procedimentos_medicos(self, df, categorias):
        """Analisa procedimentos médicos e gera relatórios"""
        try:
            print("Iniciando análise de procedimentos...")
            
            # Adicionar categoria a cada procedimento
            df['Categoria'] = df['Procedimento'].apply(
                lambda x: self.mapear_procedimento_para_categoria(x, categorias)
            )
            
            # === ANÁLISE POR PROCEDIMENTO ===
            procedimentos_agrupados = df.groupby('Procedimento').agg({
                'TotalItem': ['sum', 'count'],
                'Categoria': 'first'
            }).reset_index()
            
            procedimentos_agrupados.columns = ['Procedimento', 'Total', 'Quantidade', 'Categoria']
            
            # Adicionar informações por unidade para cada procedimento
            procedimentos_detalhados = []
            for _, proc in procedimentos_agrupados.iterrows():
                unidades_proc = df[df['Procedimento'] == proc['Procedimento']].groupby('Unidade').agg({
                    'TotalItem': ['sum', 'count']
                }).reset_index()
                unidades_proc.columns = ['Unidade', 'Total', 'Quantidade']
                
                unidades_dict = {}
                for _, unidade in unidades_proc.iterrows():
                    unidades_dict[unidade['Unidade']] = {
                        'total': float(unidade['Total']),
                        'quantidade': int(unidade['Quantidade'])
                    }
                
                procedimentos_detalhados.append({
                    'procedimento': proc['Procedimento'],
                    'categoria': proc['Categoria'],
                    'total': float(proc['Total']),
                    'quantidade': int(proc['Quantidade']),
                    'unidades': unidades_dict
                })
            
            procedimentos_detalhados = sorted(procedimentos_detalhados, key=lambda x: x['total'], reverse=True)
            
            # === ANÁLISE POR CATEGORIA ===
            categorias_agrupadas = df.groupby('Categoria').agg({
                'TotalItem': ['sum', 'count']
            }).reset_index()
            categorias_agrupadas.columns = ['Categoria', 'Total', 'Quantidade']
            
            # Adicionar percentuais
            total_geral = df['TotalItem'].sum()
            categorias_agrupadas['Percentual'] = (categorias_agrupadas['Total'] / total_geral) * 100
            categorias_agrupadas = categorias_agrupadas.sort_values('Total', ascending=False)
            
            # Preparar categorias detalhadas
            categorias_detalhadas = []
            for _, cat in categorias_agrupadas.iterrows():
                # Buscar procedimentos desta categoria
                procs_categoria = [p for p in procedimentos_detalhados if p['categoria'] == cat['Categoria']]
                procs_categoria = sorted(procs_categoria, key=lambda x: x['total'], reverse=True)
                
                procedimentos_lista = []
                for proc in procs_categoria:
                    procedimentos_lista.append({
                        'procedimento': proc['procedimento'],
                        'total': proc['total'],
                        'quantidade': proc['quantidade']
                    })
                
                categorias_detalhadas.append({
                    'categoria': cat['Categoria'],
                    'total': float(cat['Total']),
                    'quantidade': int(cat['Quantidade']),
                    'percentual': float(cat['Percentual']),
                    'procedimentos': procedimentos_lista
                })
            
            # === ANÁLISE POR UNIDADE ===
            unidades_agrupadas = df.groupby('Unidade').agg({
                'TotalItem': ['sum', 'count']
            }).reset_index()
            unidades_agrupadas.columns = ['Unidade', 'Total', 'Quantidade']
            unidades_agrupadas['Percentual'] = (unidades_agrupadas['Total'] / total_geral) * 100
            unidades_agrupadas = unidades_agrupadas.sort_values('Total', ascending=False)
            
            # Unidades detalhadas
            unidades_detalhadas = []
            for _, unidade in unidades_agrupadas.iterrows():
                # Buscar categorias desta unidade
                cats_unidade = df[df['Unidade'] == unidade['Unidade']].groupby('Categoria').agg({
                    'TotalItem': ['sum', 'count']
                }).reset_index()
                cats_unidade.columns = ['Categoria', 'Total', 'Quantidade']
                cats_unidade = cats_unidade.sort_values('Total', ascending=False)
                
                categorias_lista = []
                for _, cat in cats_unidade.iterrows():
                    categorias_lista.append({
                        'categoria': cat['Categoria'],
                        'total': float(cat['Total']),
                        'quantidade': int(cat['Quantidade'])
                    })
                
                unidades_detalhadas.append({
                    'unidade': unidade['Unidade'],
                    'total': float(unidade['Total']),
                    'quantidade': int(unidade['Quantidade']),
                    'percentual': float(unidade['Percentual']),
                    'categorias': categorias_lista
                })
            
            # Estatísticas gerais
            estatisticas = {
                'total_procedimentos': len(df),
                'total_categorias': len(categorias_agrupadas),
                'valor_total': float(total_geral),
                'total_unidades': len(unidades_agrupadas)
            }
            
            return {
                'estatisticas': estatisticas,
                'categorias': categorias_detalhadas,
                'procedimentos': procedimentos_detalhados,
                'unidades': unidades_detalhadas
            }
            
        except Exception as e:
            raise Exception(f"Erro na análise de procedimentos: {e}")

    def gerar_excel_procedimentos(self, analise, df):
        """Gera Excel com relatório de procedimentos"""
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # === ABA RESUMO ===
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE DE PROCEDIMENTOS MÉDICOS"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            # Estatísticas
            stats = analise['estatisticas']
            ws_resumo.append(["ESTATÍSTICAS GERAIS"])
            ws_resumo.append(["Total de Procedimentos", stats['total_procedimentos']])
            ws_resumo.append(["Total de Categorias", stats['total_categorias']])
            ws_resumo.append(["Valor Total", f"R$ {stats['valor_total']:,.2f}"])
            ws_resumo.append(["Total de Unidades", stats['total_unidades']])
            ws_resumo.append([])
            
            # === ABA CATEGORIAS ===
            ws_categorias = wb.create_sheet("Por Categorias")
            ws_categorias.append(["ANÁLISE POR CATEGORIAS"])
            ws_categorias.append([])
            ws_categorias.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for cat in analise['categorias']:
                ws_categorias.append([
                    cat['categoria'],
                    f"R$ {cat['total']:,.2f}",
                    cat['quantidade'],
                    f"{cat['percentual']:.1f}%"
                ])
            
            # === ABA PROCEDIMENTOS ===
            ws_procedimentos = wb.create_sheet("Por Procedimentos")
            ws_procedimentos.append(["ANÁLISE POR PROCEDIMENTOS"])
            ws_procedimentos.append([])
            ws_procedimentos.append(["Procedimento", "Categoria", "Valor Total", "Quantidade"])
            
            for proc in analise['procedimentos']:
                ws_procedimentos.append([
                    proc['procedimento'],
                    proc['categoria'],
                    f"R$ {proc['total']:,.2f}",
                    proc['quantidade']
                ])
            
            # === ABA UNIDADES ===
            ws_unidades = wb.create_sheet("Por Unidades")
            ws_unidades.append(["ANÁLISE POR UNIDADES"])
            ws_unidades.append([])
            ws_unidades.append(["Unidade", "Valor Total", "Quantidade", "Percentual"])
            
            for unidade in analise['unidades']:
                ws_unidades.append([
                    unidade['unidade'],
                    f"R$ {unidade['total']:,.2f}",
                    unidade['quantidade'],
                    f"{unidade['percentual']:.1f}%"
                ])
            
            # === ABAS DETALHADAS POR CATEGORIA ===
            for cat in analise['categorias']:
                nome_aba = cat['categoria'].replace('/', '-').replace('\\', '-')[:31]
                ws_cat = wb.create_sheet(f"Cat_{nome_aba}")
                
                ws_cat.append([f"CATEGORIA: {cat['categoria']}"])
                ws_cat.append([f"Total: R$ {cat['total']:,.2f}"])
                ws_cat.append([f"Quantidade: {cat['quantidade']} procedimentos"])
                ws_cat.append([])
                ws_cat.append(["Procedimento", "Valor", "Quantidade"])
                
                for proc in cat['procedimentos']:
                    ws_cat.append([
                        proc['procedimento'],
                        f"R$ {proc['total']:,.2f}",
                        proc['quantidade']
                    ])
            
            # Ajustar larguras das colunas
            for ws in wb.worksheets:
                for column in ws.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    ws.column_dimensions[column_letter].width = adjusted_width
            
            # Salvar
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"Erro ao gerar Excel de procedimentos: {e}")
            return None
    
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
            
            # SEMPRE mostrar TUDO - não aplicar filtro de créditos
            print(f"Mantendo todas as {len(df)} linhas (créditos + débitos)")
                
            # Debug: mostrar valores após filtro
            if len(df) > 0:
                print(f"Valores: {df['Valor'].head(10).tolist()}")
                print(f"Tipos finais - Créditos: {(df['Tipo'] == 'C').sum()}, Débitos: {(df['Tipo'] == 'D').sum()}")
            
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
        """Processa CSV do Banco do Brasil - SEMPRE retorna tudo"""
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
            # NÃO aplicar filtro - manter tudo
                
        elif 'Historico' in df.columns:
            # Formato novo
            df = df.dropna(subset=['Historico', 'Valor'])
            df = df[df['Historico'] != 'Saldo Anterior']
            
            df['Descricao'] = df['Historico']
            df['Agencia'] = df.get('Dependencia Origem', '')
            df['Documento'] = df.get('Numero do documento', '')
            df['Tipo'] = df['Valor'].apply(lambda x: 'C' if x >= 0 else 'D')
            df['Valor'] = df['Valor'].abs()
            # NÃO aplicar filtro - manter tudo
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
    
    def gerar_excel_completo(self, categorias_gerais, categorias_creditos, categorias_debitos, df_geral, df_creditos, df_debitos):
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # === ABA RESUMO GERAL ===
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE COMPLETA DE EXTRATO BANCÁRIO"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            # Estatísticas gerais
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
            
            # === ABAS DETALHADAS POR CATEGORIA ===
            
            # Função para criar aba detalhada
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
            
            # Criar abas para categorias gerais (principais)
            for resultado in categorias_gerais:
                criar_aba_categoria(resultado)
            
            # Criar abas para créditos (se houver)
            for resultado in categorias_creditos:
                criar_aba_categoria(resultado, "C_")
            
            # Criar abas para débitos (se houver)
            for resultado in categorias_debitos:
                criar_aba_categoria(resultado, "D_")
            
            # Ajustar largura das colunas dos resumos
            for ws in [ws_resumo]:
                ws.column_dimensions['A'].width = 25
                ws.column_dimensions['B'].width = 15
                ws.column_dimensions['C'].width = 12
                ws.column_dimensions['D'].width = 12
            
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
            
            # Salvar
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return base64.b64encode(excel_buffer.getvalue()).decode()
        except Exception as e:
            print(f"Erro ao gerar Excel: {e}")
            return None