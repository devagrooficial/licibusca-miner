import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Configurações Iniciais e Blindagem
load_dotenv()
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("ERRO CRÍTICO: Chaves do Supabase não encontradas no ambiente.")
    sys.exit(1) # Força o Docker a registrar o erro e reiniciar

supabase: Client = create_client(url, key)

def classificar_categoria(objeto):
    if not objeto: return 'Geral'
    t = str(objeto).lower()
    keywords = {
        'Alimentação': ['alimento', 'refeição', 'carne', 'hortifruti', 'cesta', 'merenda', 'suplemento', 'ração', 'água'],
        'Veículos e Frota': ['veículo', 'carro', 'pneu', 'peça', 'combustível', 'óleo', 'ambulância', 'caminhão'],
        'Saúde': ['saúde', 'hospital', 'medicamento', 'médico', 'clínica', 'sus'],
        'Obras e Engenharia': ['obra', 'engenharia', 'pavimentação', 'reforma', 'asfalto', 'cimento', 'reservatório', 'pavs'],
        'Tecnologia': ['computador', 'software', 'ti', 'informática', 'notebook']
    }
    for cat, keys in keywords.items():
        if any(p in t for p in keys): return cat
    return 'Geral'

def verificar_se_existe(codigo_pncp):
    try:
        # Checagem ultra-rápida de existência
        res = supabase.table("licitacoes").select("id").eq("codigo_pncp", codigo_pncp).execute()
        if len(res.data) > 0: return True
        res = supabase.table("raw_licitacoes").select("id").eq("codigo_pncp", codigo_pncp).execute()
        if len(res.data) > 0: return True
    except: pass
    return False

def buscar_itens_detalhados(cnpj, ano, seq):
    api_url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens?pagina=1&tamanhoPagina=100"
    try:
        res = requests.get(api_url, timeout=12)
        if res.status_code == 200:
            lista = res.json()
            if not isinstance(lista, list): lista = lista.get('data') or []
            return round(sum(float(i.get('valorTotal') or (float(i.get('quantidade') or 0) * float(i.get('valorUnitarioEstimado') or 0))) for i in lista), 2)
    except: pass
    return 0.0

def buscar_detalhes_capa(codigo_pncp):
    parts = codigo_pncp.replace('/', '-').split('-')
    cnpj, ano, seq = parts[0], parts[3], parts[2]
    api_url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{ano}/{seq}"
    try:
        res = requests.get(api_url, timeout=12)
        if res.status_code == 200:
            d = res.json()
            valor = float(d.get('valorTotalEstimado') or 0.0)
            if valor == 0: valor = buscar_itens_detalhados(cnpj, ano, seq)
            unidade = d.get('unidadeOrgao', {})
            return {
                "titulo": f"{d.get('modalidadeNome')} Nº {d.get('numeroCompra')}/{d.get('anoCompra')}",
                "orgao": d.get('orgaoEntidade', {}).get('razaoSocial'),
                "objeto": d.get('objetoCompra'),
                "estado": unidade.get('ufSigla'),
                "cidade": unidade.get('municipioNome'),
                "valor": valor,
                "link": d.get('linkSistemaOrigem'),
                "data_pub": d.get('dataPublicacaoPncp', '')[:10],
                "data_fim": d.get('dataEncerramentoProposta', '')[:10]
            }
    except: pass
    return None

def executar_ronda_sentinela():
    # --- MODO SENTINELA ---
    data_inicio = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
    data_fim = datetime.now().strftime('%Y%m%d')
    
    print(f"📡 Sentinela V3.2 On-line! Vigiando de {data_inicio} até {data_fim}...")
    
    pagina = 1
    novos_encontrados = 0

    while True:
        url_busca = f"https://pncp.gov.br/api/search/?tipos_documento=edital&status=recebendo_proposta&dataInicial={data_inicio}&dataFinal={data_fim}&ordenacao=-dataPublicacaoPncp&pagina={pagina}&tamanhoPagina=50"
        
        try:
            resp = requests.get(url_busca, timeout=15)
            if resp.status_code != 200: break
            
            search_items = resp.json().get('items', [])
            if not search_items: break
            
            for s_item in search_items:
                codigo = s_item.get('numero_controle_pncp')
                
                if verificar_se_existe(codigo):
                    continue 
                
                print(f"✨ Novidade detectada: {codigo}")
                det = buscar_detalhes_capa(codigo)
                if not det: continue

                ta_completo = det['valor'] > 0 and det['cidade'] and det['estado']

                if ta_completo:
                    tabela, p = "licitacoes", {"status": "aprovada", "situacao": "aberta"}
                else:
                    tabela, p = "raw_licitacoes", {"status_triagem": "pendente"}

                payload = {
                    "codigo_pncp": codigo, "titulo": det['titulo'][:255], "orgao": det['orgao'],
                    "objeto": det['objeto'], "categoria": classificar_categoria(det['objeto']),
                    "estado": det['estado'][:2] if det['estado'] else 'NI', "cidade": det['cidade'],
                    "valor_estimado": det['valor'], "link_edital": det['link'],
                    "data_publicacao": det['data_pub'], "data_encerramento": det['data_fim'], **p
                }

                try:
                    supabase.table(tabela).insert(payload).execute()
                    novos_encontrados += 1
                    tag = "✅ PROD" if ta_completo else "🏥 RAW"
                    print(f"      {tag} Salvo! {det['cidade']}-{det['estado']} | R$ {det['valor']:,.2f}")
                except: pass

            pagina += 1
            time.sleep(0.2)
        except Exception as e: 
            print(f"Aviso na paginação (Timeout ou erro de rede): {e}")
            break # Quebra a páginação, mas não mata o script todo

    print(f"🏁 Ronda finalizada. {novos_encontrados} novas oportunidades inseridas.")

if __name__ == "__main__":
    print("🚀 Minerador LiciBusca Iniciado (Modo 24/7) 🚀")
    
    while True:
        try:
            # Roda o seu código principal
            executar_ronda_sentinela()
            
            # Pausa dramática para não derrubar o PNCP (15 minutos)
            minutos = 15
            print(f"💤 Descansando por {minutos} minutos até a próxima varredura...\n")
            time.sleep(minutos * 60)
            
        except Exception as e:
            print(f"🔥 ERRO FATAL no Motor Principal: {e}")
            # O suicídio do script: Avisa o Portainer que deu ruim e manda ele reiniciar a máquina virtual
            sys.exit(1)