from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import io
import base64
import openpyxl
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
        
        response = {'status': 'OK', 'message': 'API de Procedimentos funcionando!'}
        self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        try:
            print("=== INICIANDO PROCESSAMENTO PROCEDIMENTOS ===")
            
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
            
            procedures_data = files.get('procedures_file')
            categories_data = files.get('categories_file')
            
            if not procedures_data or not categories_data:
                raise Exception("Arquivos necessários não foram enviados")
            
            print(f"Procedures: {len(procedures_data)} bytes")
            print(f"Categories: {len(categories_data)} bytes")
            
            # Processar Categorias
            print("Processando categorias...")
            categorias = self.processar_arquivo_categorias(categories_data)
            print(f"Categorias encontradas: {len(categorias)}")
            
            # Processar Procedimentos (incluindo gratuitos)
            print("Processando procedimentos (incluindo gratuitos)...")
            df = self.processar_arquivo_procedimentos(procedures_data)
            print(f"Linhas processadas: {len(df)}")
            
            # Categorizar
            print("Categorizando procedimentos...")
            df['Categoria'] = df['Procedimento'].apply(lambda x: self.mapear_procedimento_para_categoria(x, categorias))
            
            # Separar estatísticas
            procedimentos_pagos = df[df['TotalItem'] > 0]
            procedimentos_gratuitos = df[df['TotalItem'] == 0]
            
            print(f"Total procedimentos: {len(df)} registros")
            print(f"Procedimentos pagos: {len(procedimentos_pagos)}")
            print(f"Procedimentos gratuitos: {len(procedimentos_gratuitos)}")
            
            # Agrupar resultados GERAIS (todos os procedimentos)
            print("Agrupando resultados gerais...")
            resultados_gerais = df.groupby('Categoria').agg({
                'TotalItem': ['sum', 'count']
            }).reset_index()
            resultados_gerais.columns = ['categoria', 'total', 'quantidade']
            valor_total = df['TotalItem'].sum()
            
            if valor_total > 0:
                resultados_gerais['percentual'] = (resultados_gerais['total'] / valor_total) * 100
            else:
                resultados_gerais['percentual'] = 0
            resultados_gerais = resultados_gerais.sort_values('total', ascending=False)
            
            # Agrupar resultados por PROCEDIMENTO
            print("Agrupando resultados por procedimento...")
            resultados_procedimentos = df.groupby('Procedimento').agg({
                'TotalItem': ['sum', 'count'],
                'Categoria': 'first'
            }).reset_index()
            resultados_procedimentos.columns = ['procedimento', 'total', 'quantidade', 'categoria']
            resultados_procedimentos = resultados_procedimentos.sort_values('total', ascending=False)
            
            # Agrupar resultados por UNIDADE
            print("Agrupando resultados por unidade...")
            resultados_unidades = df.groupby('Unidade').agg({
                'TotalItem': ['sum', 'count']
            }).reset_index()
            resultados_unidades.columns = ['unidade', 'total', 'quantidade']
            if valor_total > 0:
                resultados_unidades['percentual'] = (resultados_unidades['total'] / valor_total) * 100
            else:
                resultados_unidades['percentual'] = 0
            resultados_unidades = resultados_unidades.sort_values('total', ascending=False)
            
            # Preparar respostas detalhadas
            print("Preparando respostas...")
            categorias_gerais = self.preparar_categorias_detalhadas(resultados_gerais, df, 'categoria')
            procedimentos_detalhados = self.preparar_categorias_detalhadas(resultados_procedimentos, df, 'procedimento')
            unidades_detalhadas = self.preparar_categorias_detalhadas(resultados_unidades, df, 'unidade')
            
            # Gerar Excel
            print("Gerando Excel...")
            excel_b64 = self.gerar_excel_procedimentos(categorias_gerais, procedimentos_detalhados, unidades_detalhadas, df)
            
            resposta = {
                'success': True,
                'estatisticas': {
                    'total_procedimentos': len(df),
                    'total_categorias': len(resultados_gerais),
                    'valor_total': float(valor_total),
                    'total_unidades': len(resultados_unidades),
                    'procedimentos_pagos': len(procedimentos_pagos),
                    'procedimentos_gratuitos': len(procedimentos_gratuitos),
                    'valor_total_pagos': float(procedimentos_pagos['TotalItem'].sum() if len(procedimentos_pagos) > 0 else 0)
                },
                'categorias': categorias_gerais,
                'procedimentos': procedimentos_detalhados,
                'unidades': unidades_detalhadas,
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

    def processar_arquivo_categorias(self, categories_data):
        """Processa arquivo Excel de categorias de forma robusta"""
        try:
            df = pd.read_excel(io.BytesIO(categories_data))
            print(f"Arquivo de categorias carregado: {df.shape}")
            
            categorias = []
            
            # Tentar diferentes abordagens para extrair categorias
            for col_idx in range(min(3, df.shape[1])):
                for _, row in df.iterrows():
                    if pd.notna(row.iloc[col_idx]):
                        valor = str(row.iloc[col_idx]).strip()
                        if valor and len(valor) > 1 and valor not in categorias:
                            if not valor.isdigit() and not valor.startswith('Unnamed'):
                                categorias.append(valor)
            
            # Se encontrou poucas categorias, expandir busca
            if len(categorias) < 5:
                print("Poucas categorias encontradas, expandindo busca...")
                for col in df.columns:
                    for _, row in df.iterrows():
                        if pd.notna(row[col]):
                            valor = str(row[col]).strip()
                            if (valor and len(valor) > 2 and valor not in categorias 
                                and not valor.isdigit() and not valor.startswith('Unnamed')):
                                categorias.append(valor)
            
            # Remover duplicatas e limpar
            categorias = list(set(categorias))
            categorias = [cat for cat in categorias if len(cat) > 2]
            
            print(f"Categorias encontradas ({len(categorias)}): {categorias[:10]}...")
            return categorias
            
        except Exception as e:
            print(f"Erro ao processar categorias: {e}")
            return ["CONSULTAS", "EXAMES", "PROCEDIMENTOS", "MEDICAMENTOS", "OUTROS"]

    def processar_arquivo_procedimentos(self, procedures_data):
        """Processa arquivo de procedimentos INCLUINDO valores zero (gratuitos)"""
        try:
            print("=== PROCESSAMENTO INCLUINDO PROCEDIMENTOS GRATUITOS ===")
            
            # Ler arquivo sem assumir cabeçalhos
            df_raw = pd.read_excel(io.BytesIO(procedures_data), header=None)
            print(f"Arquivo carregado: {df_raw.shape}")
            
            # Detectar colunas automaticamente
            unidade_col, procedimento_col, valor_col = self.detectar_colunas(df_raw)
            print(f"Colunas detectadas: Unidade=Col{unidade_col}, Procedimento=Col{procedimento_col}, Valor=Col{valor_col}")
            
            # Extrair dados
            dados_extraidos = []
            linhas_processadas = 0
            
            print("Extraindo dados (incluindo gratuitos)...")
            for i in range(len(df_raw)):
                try:
                    unidade = df_raw.iloc[i, unidade_col] if unidade_col < df_raw.shape[1] else None
                    procedimento = df_raw.iloc[i, procedimento_col] if procedimento_col < df_raw.shape[1] else None
                    valor = df_raw.iloc[i, valor_col] if valor_col < df_raw.shape[1] else None
                    
                    # Pular cabeçalhos
                    if (pd.isna(procedimento) or 
                        str(procedimento).upper().strip() in ['PROCEDIMENTO', 'DESCRICAO', 'PROC', '']):
                        continue
                    
                    # Limpar dados
                    unidade_clean = str(unidade).strip() if pd.notna(unidade) else "Não informado"
                    procedimento_clean = str(procedimento).strip()
                    valor_clean = self.converter_valor_robusto(valor)
                    
                    # ✅ INCLUIR TODOS - mesmo com valor 0
                    if len(procedimento_clean) > 3:
                        dados_extraidos.append({
                            'Unidade': unidade_clean,
                            'Procedimento': procedimento_clean,
                            'TotalItem': valor_clean
                        })
                        linhas_processadas += 1
                        
                        # Log primeiras linhas
                        if linhas_processadas <= 5:
                            status = "GRATUITO" if valor_clean == 0 else f"R$ {valor_clean:,.2f}"
                            print(f"  Linha {i}: {procedimento_clean[:40]}... | {status}")
                
                except Exception as e:
                    continue
            
            if len(dados_extraidos) == 0:
                raise Exception("Nenhum dado válido encontrado")
            
            df_final = pd.DataFrame(dados_extraidos)
            
            # Estatísticas finais
            pagos = df_final[df_final['TotalItem'] > 0]
            gratuitos = df_final[df_final['TotalItem'] == 0]
            
            print(f"✅ PROCESSAMENTO CONCLUÍDO:")
            print(f"   📊 Total: {len(df_final)} procedimentos")
            print(f"   💰 Pagos: {len(pagos)} (R$ {pagos['TotalItem'].sum():,.2f})")
            print(f"   🆓 Gratuitos: {len(gratuitos)}")
            print(f"   🏢 Unidades: {df_final['Unidade'].nunique()}")
            
            return df_final
            
        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
            raise Exception(f"Erro ao processar procedimentos: {e}")

    def detectar_colunas(self, df_raw):
        """Detecta automaticamente as colunas de Unidade, Procedimento e Valor"""
        unidade_col = None
        procedimento_col = None
        valor_col = None
        
        # Buscar por palavras-chave
        for i in range(min(10, len(df_raw))):
            for j in range(min(15, df_raw.shape[1])):
                cell_value = str(df_raw.iloc[i, j]).upper().strip() if pd.notna(df_raw.iloc[i, j]) else ""
                
                if ('UNIDADE' in cell_value or 'UNIT' in cell_value) and unidade_col is None:
                    unidade_col = j
                elif ('PROCEDIMENTO' in cell_value or 'PROC' in cell_value) and procedimento_col is None:
                    procedimento_col = j
                elif ('TOTAL' in cell_value and 'ITEM' in cell_value) and valor_col is None:
                    valor_col = j
        
        # Fallback para posições conhecidas
        if unidade_col is None:
            unidade_col = 0
        if procedimento_col is None:
            procedimento_col = 5 if df_raw.shape[1] > 5 else min(2, df_raw.shape[1]-1)
        if valor_col is None:
            valor_col = min(10, df_raw.shape[1]-1)
        
        return unidade_col, procedimento_col, valor_col

    def converter_valor_robusto(self, valor):
        """Converte valores PERMITINDO zeros (procedimentos gratuitos)"""
        if pd.isna(valor):
            return 0.0
        
        try:
            if isinstance(valor, (int, float)):
                return float(valor)  # Permite zero
            
            valor_str = str(valor).strip()
            if not valor_str:
                return 0.0
            
            # Limpar caracteres não numéricos
            import re
            valor_clean = re.sub(r'[^\d,.-]', '', valor_str)
            
            if not valor_clean:
                return 0.0
            
            # Tratar vírgulas e pontos
            if ',' in valor_clean and '.' in valor_clean:
                if valor_clean.rfind(',') > valor_clean.rfind('.'):
                    valor_clean = valor_clean.replace('.', '').replace(',', '.')
                else:
                    valor_clean = valor_clean.replace(',', '')
            elif ',' in valor_clean:
                valor_clean = valor_clean.replace(',', '.')
            
            return float(valor_clean)  # Retorna valor original (pode ser 0)
            
        except Exception as e:
            return 0.0

    def mapear_procedimento_para_categoria(self, procedimento, categorias):
        """Mapeia procedimento para categoria"""
        if not procedimento:
            return "Outros"
        
        proc_upper = str(procedimento).upper()
        
        # Mapeamentos específicos
        mapeamentos = {
            'CONSULTA': 'CONSULTAS',
            'EXAM': 'EXAMES',
            'ULTRA': 'EXAMES',
            'RAIO': 'EXAMES',
            'VITAMINA': 'MEDICAMENTOS',
            'MEDICAMENTO': 'MEDICAMENTOS',
            'CIRURGIA': 'PROCEDIMENTOS'
        }
        
        for palavra, categoria in mapeamentos.items():
            if palavra in proc_upper:
                return categoria
        
        # Verificar categorias fornecidas
        for categoria in categorias:
            if str(categoria).upper().strip() in proc_upper:
                return categoria
        
        return "Outros"

    def preparar_categorias_detalhadas(self, resultados, dataframe, tipo):
        """Prepara dados detalhados para resposta"""
        categorias_detalhadas = []
        
        for _, row in resultados.iterrows():
            if tipo == 'categoria':
                categoria = row['categoria']
                itens_cat = dataframe[dataframe['Categoria'] == categoria]
                
                procs_categoria = itens_cat.groupby('Procedimento').agg({
                    'TotalItem': ['sum', 'count']
                }).reset_index()
                procs_categoria.columns = ['procedimento', 'total', 'quantidade']
                procs_categoria = procs_categoria.sort_values('total', ascending=False)
                
                procedimentos_lista = []
                for _, proc in procs_categoria.iterrows():
                    procedimentos_lista.append({
                        'procedimento': proc['procedimento'],
                        'total': float(proc['total']),
                        'quantidade': int(proc['quantidade'])
                    })
                
                categorias_detalhadas.append({
                    'categoria': categoria,
                    'total': float(row['total']),
                    'quantidade': int(row['quantidade']),
                    'percentual': float(row['percentual']),
                    'procedimentos': procedimentos_lista
                })
                
            elif tipo == 'procedimento':
                procedimento = row['procedimento']
                itens_proc = dataframe[dataframe['Procedimento'] == procedimento]
                
                unidades_proc = itens_proc.groupby('Unidade').agg({
                    'TotalItem': ['sum', 'count']
                }).reset_index()
                unidades_proc.columns = ['unidade', 'total', 'quantidade']
                
                unidades_dict = {}
                for _, unidade in unidades_proc.iterrows():
                    unidades_dict[unidade['unidade']] = {
                        'total': float(unidade['total']),
                        'quantidade': int(unidade['quantidade'])
                    }
                
                categorias_detalhadas.append({
                    'procedimento': procedimento,
                    'categoria': row['categoria'],
                    'total': float(row['total']),
                    'quantidade': int(row['quantidade']),
                    'unidades': unidades_dict
                })
                
            elif tipo == 'unidade':
                unidade = row['unidade']
                itens_unidade = dataframe[dataframe['Unidade'] == unidade]
                
                cats_unidade = itens_unidade.groupby('Categoria').agg({
                    'TotalItem': ['sum', 'count']
                }).reset_index()
                cats_unidade.columns = ['categoria', 'total', 'quantidade']
                cats_unidade = cats_unidade.sort_values('total', ascending=False)
                
                categorias_lista = []
                for _, cat in cats_unidade.iterrows():
                    categorias_lista.append({
                        'categoria': cat['categoria'],
                        'total': float(cat['total']),
                        'quantidade': int(cat['quantidade'])
                    })
                
                categorias_detalhadas.append({
                    'unidade': unidade,
                    'total': float(row['total']),
                    'quantidade': int(row['quantidade']),
                    'percentual': float(row['percentual']),
                    'categorias': categorias_lista
                })
        
        return categorias_detalhadas

    def gerar_excel_procedimentos(self, categorias_gerais, procedimentos_detalhados, unidades_detalhadas, df):
        """Gera Excel completo incluindo procedimentos gratuitos"""
        try:
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # RESUMO GERAL
            ws_resumo = wb.create_sheet("Resumo Geral")
            ws_resumo.append(["ANÁLISE COMPLETA DE PROCEDIMENTOS MÉDICOS (INCLUINDO GRATUITOS)"])
            ws_resumo.append([f"Gerado em: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}"])
            ws_resumo.append([])
            
            # Estatísticas
            total_procedimentos = len(df)
            procedimentos_pagos = len(df[df['TotalItem'] > 0])
            procedimentos_gratuitos = len(df[df['TotalItem'] == 0])
            valor_total = df['TotalItem'].sum()
            
            ws_resumo.append(["ESTATÍSTICAS GERAIS"])
            ws_resumo.append(["Total de Procedimentos", total_procedimentos])
            ws_resumo.append(["Procedimentos Pagos", procedimentos_pagos])
            ws_resumo.append(["Procedimentos Gratuitos", procedimentos_gratuitos])
            ws_resumo.append(["% Procedimentos Gratuitos", f"{(procedimentos_gratuitos/total_procedimentos)*100:.1f}%"])
            ws_resumo.append(["Valor Total (só pagos)", f"R$ {valor_total:,.2f}"])
            ws_resumo.append([])
            
            ws_resumo.append(["RESUMO POR CATEGORIA"])
            ws_resumo.append(["Categoria", "Valor Total", "Quantidade", "Percentual"])
            
            for resultado in categorias_gerais:
                ws_resumo.append([
                    resultado['categoria'],
                    f"R$ {resultado['total']:,.2f}",
                    resultado['quantidade'],
                    f"{resultado['percentual']:.1f}%"
                ])
            
            # DADOS BRUTOS
            ws_dados = wb.create_sheet("Dados Brutos")
            ws_dados.append(["TODOS OS PROCEDIMENTOS (PAGOS + GRATUITOS)"])
            ws_dados.append([])
            ws_dados.append(["#", "Unidade", "Procedimento", "Categoria", "Valor", "Tipo"])
            
            for i, row in df.iterrows():
                tipo_servico = "GRATUITO" if row['TotalItem'] == 0 else "PAGO"
                valor_formatado = "R$ 0,00" if row['TotalItem'] == 0 else f"R$ {row['TotalItem']:,.2f}"
                
                ws_dados.append([
                    i + 1,
                    row['Unidade'],
                    row['Procedimento'],
                    row['Categoria'],
                    valor_formatado,
                    tipo_servico
                ])
            
            # ESTATÍSTICAS DETALHADAS
            ws_stats = wb.create_sheet("Estatísticas Detalhadas")
            ws_stats.append(["ANÁLISE DETALHADA POR CATEGORIA"])
            ws_stats.append([])
            ws_stats.append(["Categoria", "Total", "Pagos", "Gratuitos", "% Gratuitos", "Valor"])
            
            for categoria in df['Categoria'].unique():
                cat_data = df[df['Categoria'] == categoria]
                cat_total = len(cat_data)
                cat_pagos = len(cat_data[cat_data['TotalItem'] > 0])
                cat_gratuitos = len(cat_data[cat_data['TotalItem'] == 0])
                cat_valor = cat_data['TotalItem'].sum()
                cat_perc = (cat_gratuitos / cat_total) * 100 if cat_total > 0 else 0
                
                ws_stats.append([
                    categoria,
                    cat_total,
                    cat_pagos,
                    cat_gratuitos,
                    f"{cat_perc:.1f}%",
                    f"R$ {cat_valor:,.2f}"
                ])
            
            # Ajustar larguras
            ws_resumo.column_dimensions['A'].width = 25
            ws_resumo.column_dimensions['B'].width = 15
            
            ws_dados.column_dimensions['A'].width = 5
            ws_dados.column_dimensions['B'].width = 25
            ws_dados.column_dimensions['C'].width = 60
            ws_dados.column_dimensions['D'].width = 20
            ws_dados.column_dimensions['E'].width = 15
            ws_dados.column_dimensions['F'].width = 10
            
            ws_stats.column_dimensions['A'].width = 25
            ws_stats.column_dimensions['B'].width = 8
            ws_stats.column_dimensions['C'].width = 8
            ws_stats.column_dimensions['D'].width = 10
            ws_stats.column_dimensions['E'].width = 12
            ws_stats.column_dimensions['F'].width = 15
            
            # Salvar
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            print(f"✅ Excel gerado com {len(wb.worksheets)} abas")
            return base64.b64encode(excel_buffer.getvalue()).decode()
            
        except Exception as e:
            print(f"❌ Erro ao gerar Excel: {e}")
            return None