"""Gera, para cada figura do app de observabilidade, um CSV com exatamente os dados plotados.

Uso:  python3 gerar-dados-graficos.py <catalogo.schema> <EXP_ID> <pasta_saida> [--quarentena]
  ex.: python3 gerar-dados-graficos.py well_costa_classic_stable_catalog.tcc_dq_exp  EXP-2026-10-04 dados-graficos
       python3 gerar-dados-graficos.py well_costa_classic_stable_catalog.tcc_dq_exp2 EXP-2026-11-09 dados-graficos --quarentena
Requer a Databricks CLI autenticada (perfil em DATABRICKS_PROFILE, padrão fevm-classic-stable) e DATABRICKS_WAREHOUSE_ID.
"""
import json, os, subprocess, sys, time
import pandas as pd

PERFIL = os.environ.get("DATABRICKS_PROFILE", "fevm-classic-stable")
WH = os.environ.get("DATABRICKS_WAREHOUSE_ID", "321cb5c7542d48a3")
S, EXP, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
QUAR = "--quarentena" in sys.argv
os.makedirs(OUT, exist_ok=True)

DIM = {"COM": "Completude", "CON": "Conformidade", "VAL": "Validade", "VALIMP": "Validade (data impossível)",
       "UNI": "Unicidade", "CONTROLE": "Controle (sem defeito)"}
ORIG = {"BRZ": "fonte", "SLV": "transformação"}
dim_de = lambda c: DIM[c] if c == "CONTROLE" else DIM[c.rsplit("-", 1)[0]]
origem_de = lambda c: "" if c == "CONTROLE" else ORIG[c.rsplit("-", 1)[1]]


def api(*args):
    return json.loads(subprocess.run(["databricks", "--profile", PERFIL, "api", *args], capture_output=True, text=True, check=True).stdout)


def q(sql):
    r = api("post", "/api/2.0/sql/statements", "--json",
            json.dumps({"statement": sql, "warehouse_id": WH, "wait_timeout": "50s", "format": "JSON_ARRAY", "disposition": "INLINE"}))
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3); r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"])
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return pd.DataFrame(r.get("result", {}).get("data_array", []), columns=cols)


def salva(df, nome, descricao, indice):
    df.to_csv(f"{OUT}/{nome}.csv", index=False)
    indice.append((f"{nome}.csv", len(df), descricao))


idx = []
# fig-1: tarefas do job
t = q(f"select task_key tarefa, inicio, fim, resultado from {S}.obs_job_tarefas order by inicio")
t["duracao_min"] = ((pd.to_datetime(t.fim) - pd.to_datetime(t.inicio)).dt.total_seconds() / 60).round(2)
salva(t, "fig-1-execucao-job", "tarefas do job: início, fim, duração (min) e resultado", idx)

# fig-2: matriz cenário × regra × ponto
m = q(f"select cenario_id, regra_id, etapa, sum(cast(n_violacoes as bigint)) registros_sinalizados from {S}.dq_resumo_regras group by all")
m["registros_sinalizados"] = m.registros_sinalizados.astype(int)
m["registros_com_defeito"] = m.cenario_id.map(lambda c: 0 if c == "CONTROLE" else (190180 if c.startswith("UNI") else 95090))
m["fracao_sinalizada"] = (m.registros_sinalizados / m.registros_com_defeito.where(m.registros_com_defeito > 0)).round(6)
m.insert(1, "dimensao_do_defeito", m.cenario_id.map(dim_de)); m.insert(2, "origem_do_defeito", m.cenario_id.map(origem_de))
salva(m.sort_values(["cenario_id", "regra_id", "etapa"]), "fig-2-matriz-deteccao",
      "registros sinalizados por cenário × regra × ponto (soma de 5 lotes) e fração dos registros com defeito", idx)

# fig-3 e fig-5: marcas de tempo de cada lote (relógio do controlador), em segundos desde o início do lote
man = q(f"select cast(ordem as int) ordem, cenario_id, run_id, manifesto from {S}.manifesto_execucao")
chaves = ["bronze", "t1_eventos", "alerta_T1", "silver", "t2_eventos", "alerta_T2"] + (["quarentena"] if QUAR else []) + ["gold_inicio", "gold_fim"]
linhas = []
for _, r in man.iterrows():
    mk = json.loads(r.manifesto)["marcas"]; t0 = pd.Timestamp(mk["inicio"])
    d = {"ordem": int(r.ordem), "run_id": r.run_id, "cenario_id": r.cenario_id}
    for k in chaves:
        d[f"{k}_s"] = round((pd.Timestamp(mk[k]) - t0).total_seconds(), 3) if mk.get(k) else None
    d["houve_alerta"] = bool(mk.get("alerta_T1_versao") is not None or mk.get("alerta_T2_versao") is not None)
    linhas.append(d)
lt = pd.DataFrame(linhas).sort_values("ordem")
salva(lt, "fig-3-linha-do-tempo-lotes", "por lote: segundos desde o início até cada marco (Bronze, T1, alertas, Silver, T2"
      + (", quarentena" if QUAR else "") + ", início e fim da Gold)", idx)

tp = q(f"select cast(ordem as int) ordem, cenario_id, condicao, cast(duracao_s as double) duracao_s from {S}.tempo_execucao")
tp["ordem"], tp["duracao_s"] = tp.ordem.astype(int), tp.duracao_s.astype(float)
salva(tp.sort_values(["ordem", "condicao"]), "fig-5a-duracao-lotes", "duração de cada lote com e sem verificação (s)", idx)
etp = pd.DataFrame({"Ingestão Bronze": lt.bronze_s, "Verificação T1": lt.t1_eventos_s - lt.bronze_s,
                    "Alertas e Silver": lt.silver_s - lt.t1_eventos_s, "Verificação T2": lt.t2_eventos_s - lt.silver_s,
                    ("Alerta T2, quarentena e Gold" if QUAR else "Alerta T2 e Gold"): lt.gold_fim_s - lt.t2_eventos_s}).median()
med = tp.groupby("condicao").duracao_s.median()
comp = pd.DataFrame({"execucao": "com verificações", "etapa": etp.index, "mediana_s": etp.values.round(3)})
comp = pd.concat([comp, pd.DataFrame([{"execucao": "com verificações", "etapa": "TOTAL (mediana do lote)", "mediana_s": round(med["com_gx"], 3)},
                                      {"execucao": "sem verificações", "etapa": "TOTAL (mediana do lote)", "mediana_s": round(med["sem_gx"], 3)}])])
salva(comp, "fig-5b-composicao-tempo", "mediana do tempo por etapa (com verificação) e total com × sem verificação", idx)

# fig-4
if QUAR:
    qr = q(f"""select cast(ordem as int) ordem, cenario_id, n_silver, n_sinalizados, n_quarentena, sinalizados_na_gold, row_ids_linhagem registros_na_gold,
               quarentena_igual_sinalizados, gold_sem_sinalizados, quarentena_mais_gold_igual_silver, quarentena_igual_oraculo,
               gold_igual_referencia, diferencas_gold_vs_referencia from {S}.resultado_quarentena order by ordem""")
    qr.insert(2, "dimensao_do_defeito", qr.cenario_id.map(dim_de))
    salva(qr, "fig-4-quarentena-gold", "por lote: sinalizados, em quarentena, sinalizados na Gold e as 5 conferências da quarentena", idx)
else:
    g = q(f"select cast(ordem as int) ordem, cenario_id, grupos_total, grupos_afetados, registros_gold, registros_sinalizados from {S}.obs_gold_afetada order by ordem")
    g.insert(2, "dimensao_do_defeito", g.cenario_id.map(dim_de))
    salva(g, "fig-4-gold-grupos-afetados", "por lote: grupos da Gold (município × mês) e quantos receberam registro sinalizado", idx)

# fig-6/7: amostras usadas nas tabelas
salva(q(f"select cenario_id, etapa, regra_id, dimensao, n_violacoes, instante from {S}.dq_alertas order by instante"),
      "fig-6a-metadados-alertas", "todos os alertas emitidos", idx)
salva(q(f"select * from {S}.dq_eventos where run_id = '{EXP}-L02-UNI-BRZ-S45' order by row_id, etapa limit 200"),
      "fig-6b-metadados-eventos", "amostra de 200 eventos do lote 2 (unicidade)", idx)
if QUAR:
    salva(q(f"select * from {S}.dq_quarentena where run_id = '{EXP}-L14-VALIMP-BRZ-S43' order by row_id limit 200"),
          "fig-6c-quarentena", "amostra de 200 registros em quarentena do lote 14 (data impossível)", idx)
salva(q(f"""select s.row_id, r.valor_antes data_original, s.DT_NOTIFIC data_recebida_texto, s.data_notificacao data_convertida,
           s.falha_conv_data_notificacao falha_conversao, array_join(s.violacoes_t1, ',') regras_violadas_t1
           from {S}.silver_lote s join {S}.referencia_experimental r on r.run_id = s.run_id and r.row_id = s.row_id
           where s.run_id = '{EXP}-L14-VALIMP-BRZ-S43' order by s.row_id limit 200"""),
      "fig-7-data-impossivel", "amostra de 200 registros com data impossível na Silver (lote 14)", idx)

with open(f"{OUT}/INDICE.csv", "w") as f:
    pd.DataFrame(idx, columns=["arquivo", "linhas", "conteudo"]).to_csv(f, index=False)
print("\n".join(f"{a}  {n}" for a, n, _ in idx))
