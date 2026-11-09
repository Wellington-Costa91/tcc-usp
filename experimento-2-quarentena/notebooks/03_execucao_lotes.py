# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Execução dos lotes experimentais
# MAGIC Por lote (cenário × semente):
# MAGIC 1. **Injeção → landing**: cópia intacta da referência; nos cenários de fonte (`*-BRZ`) recebe o defeito; o lote é gravado como
# MAGIC    arquivo em `/Volumes/.../landing/<run_id>/`, simulando a entrega da fonte.
# MAGIC 2. **Ingestão → Bronze**: lê o arquivo que pousou e grava na Bronze como chegou (com os defeitos) + metadados de ingestão.
# MAGIC 3. **Bronze → Silver (T1)**: GX sobre a Bronze, captura dos inválidos e alerta; Silver gravada com todos os registros, marcados.
# MAGIC    Nos cenários de transformação (`*-SLV`) a etapa simula uma falha que introduz o defeito no que a Silver produz.
# MAGIC 4. **Silver → Gold (T2)**: GX sobre a Silver, captura e alerta.
# MAGIC 5. **Quarentena (experimento 2)**: todo registro sinalizado em T1 ou T2 vai para `dq_quarentena`, com as regras violadas;
# MAGIC    **só então** a Gold é construída, apenas com os registros não sinalizados, + linhagem.
# MAGIC Módulo de tempo: cada lote tem uma execução pareada sem GX (ordem alternada), após 2 aquecimentos por condição.

# COMMAND ----------
# MAGIC %run ./00_comum

# COMMAND ----------
import time, random, traceback
dbutils.widgets.text("limite_lotes", "0")       # 0 = todos
dbutils.widgets.text("modulo_tempo", "true")
LIMITE = int(dbutils.widgets.get("limite_lotes")); MODULO_TEMPO = dbutils.widgets.get("modulo_tempo") == "true"
ibge = dominio_ibge()
ibge_df = spark.createDataFrame([(c,) for c in ibge], ["cod6"])
SUITES = {"T1": suite_gx("T1", ibge), "T2": suite_gx("T2", ibge)}
H_REGRAS = hash_regras(ibge)
congel = json.loads(spark.table(f"{FQ}.regras_congeladas").first()["definicao"])
assert congel["congelado"] and congel["hash_regras"] == H_REGRAS, "regras divergem da versao congelada"
par = json.loads(spark.table(f"{FQ}.manifesto_experimento").first()["parametros"])
N, M, H_AMOSTRA = par["N"], par["m"], par["hash_amostra"]
REFERENCIA = f"{FQ}.prep_referencia"
print("N =", N, "m =", M)

# COMMAND ----------
# Lotes: controle + 4 dimensões × 2 origens do defeito (BRZ = chega pela fonte, avaliado em T1; SLV = falha na
# transformação Bronze→Silver, avaliado em T2) + data impossível na fonte; 5 sementes; ordem pseudoaleatória registrada
SIGLA = {"completude": "COM", "conformidade": "CON", "validade": "VAL", "unicidade": "UNI"}
lotes = [("CONTROLE", None, None, s, "principal") for s in SEMENTES]
for ponto, sufixo in (("T1", "BRZ"), ("T2", "SLV")):
    for dim in ("completude", "conformidade", "validade", "unicidade"):
        lotes += [(f"{SIGLA[dim]}-{sufixo}", dim, ponto, s, "principal") for s in SEMENTES]
lotes += [("VALIMP-BRZ", "validade", "T1", s, "data_impossivel") for s in SEMENTES]
random.Random(SEMENTE_ORDEM).shuffle(lotes)
if LIMITE: lotes = lotes[:LIMITE]

PRINC = {k: f"{FQ}.{k}" for k in ["bronze_lote", "dq_eventos", "dq_resumo_regras", "dq_alertas", "silver_lote", "gold_notif_mun_mes",
                                  "gold_linhagem", "referencia_experimental", "selecao_lotes", "oraculo_violacoes",
                                  "aplicabilidade", "manifesto_execucao", "tempo_execucao", "dq_quarentena"]}
SCR = {k: f"{FQ}.tmp_{k}" for k in ["bronze_lote", "dq_eventos", "dq_resumo_regras", "dq_alertas", "silver_lote", "gold_notif_mun_mes",
                                  "dq_quarentena"]}
# rodada nova: nenhum produto de execução anterior é reaproveitado como resultado atual
for tb in list(PRINC.values()) + list(SCR.values()):
    spark.sql(f"DROP TABLE IF EXISTS {tb}")
dbutils.fs.rm(LANDING, recurse=True); dbutils.fs.mkdirs(LANDING)

def grava(df, tabela, modo="append"):
    df.write.mode(modo).option("mergeSchema", "true").saveAsTable(tabela)

# COMMAND ----------
def entrega_fonte(lote, run_id):
    """Fonte simulada (fora do pipeline): copia a referência, aplica o defeito de fonte e entrega o arquivo no landing."""
    cen, dim, ponto, sem, variante = lote
    base = spark.table(REFERENCIA).drop("chave_conteudo")
    sel = selecionar(base, sem, M) if dim else None
    ref = None
    if dim and ponto == "T1":
        base, ref = injetar(base, dim, "T1", sel, sem, variante)
    destino = f"{LANDING}/{run_id}"
    base.write.mode("overwrite").parquet(destino)
    return destino, sel, ref

def executa(lote, ordem, destino, sel, com_gx=True, T=PRINC, scratch=False):
    """Pipeline medido: ingestão do arquivo pousado → Bronze → T1 → Silver → T2 → Gold."""
    cen, dim, ponto, sem, variante = lote
    run_id = f"{EXP_ID}-L{ordem:02d}-{cen}-S{sem}" + ("" if com_gx else "-SEMGX") + ("-AQUEC" if scratch else "")
    ctx = {"run_id": run_id, "cenario_id": cen, "semente": sem, "seq": {"T1": 1, "T2": 2}}
    marcas = {}; modo = "overwrite" if (scratch or not com_gx) else "append"
    t0 = time.monotonic(); marcas["inicio"] = utc_now()
    # ---- Ingestão: o arquivo entra na Bronze como chegou (com os defeitos), só com metadados de ingestão
    tb = T["bronze_lote"] if com_gx else SCR["bronze_lote"]
    bronze_in = (spark.read.parquet(destino).withColumn("_arquivo_landing", F.col("_metadata.file_path"))
                 .withColumn("_ingerido_em", F.current_timestamp()).withColumn("run_id", F.lit(run_id)))
    grava(bronze_in, tb, modo); marcas["bronze"] = utc_now()
    bronze = spark.table(tb).where(F.col("run_id") == run_id)
    lote_df = bronze.select(*COLS_LOTE)
    ref = None
    if com_gx:
        ctx["ref_entrada"] = hash_lote(lote_df)
        # ---- Bronze → Silver: a Silver analisa a Bronze (T1), captura os inválidos e registra o alerta
        v1 = aplica_regras(lote_df, SUITES["T1"]); ctx["instante"] = utc_now()
        grava(eventos(v1, "T1", ctx), T["dq_eventos"], modo); marcas["t1_eventos"] = utc_now()
        resumo_alerta(T, run_id, cen, "T1", marcas, scratch)
    silver = para_silver(lote_df)
    if dim and ponto == "T2":   # falha simulada da transformação Bronze → Silver
        silver, ref = injetar(silver, dim, "T2", sel, sem, variante)
    if com_gx:
        marca_t1 = (spark.table(T["dq_eventos"]).where((F.col("run_id") == run_id) & (F.col("etapa") == "T1"))
                    .groupBy("row_id").agg(F.array_sort(F.collect_set("regra_id")).alias("violacoes_t1")))
        silver = silver.join(marca_t1, "row_id", "left")
    ts = T["silver_lote"] if com_gx else SCR["silver_lote"]
    grava(silver.withColumn("run_id", F.lit(run_id)), ts, modo); marcas["silver"] = utc_now()
    silver_t = spark.table(ts).where(F.col("run_id") == run_id).drop("run_id")
    if com_gx:
        ctx["ref_entrada"] = hash_lote(silver_t.drop("violacoes_t1"))
        # ---- Silver → Gold: validação T2 do que a Silver produziu, antes de materializar a Gold
        v2 = aplica_regras(silver_t, SUITES["T2"]); ctx["instante"] = utc_now()
        grava(eventos(v2, "T2", ctx), T["dq_eventos"], "append"); marcas["t2_eventos"] = utc_now()
        resumo_alerta(T, run_id, cen, "T2", marcas, scratch)
    if com_gx:
        # ---- Quarentena: registros sinalizados em T1 ou T2 saem do caminho da Gold e ficam guardados para revisão
        sinal = (spark.table(T["dq_eventos"]).where(F.col("run_id") == run_id)
                 .groupBy("row_id").agg(F.array_sort(F.collect_set(F.concat_ws("@", "regra_id", "etapa"))).alias("violacoes")))
        quar = (silver_t.join(sinal, "row_id", "inner").withColumn("run_id", F.lit(run_id)).withColumn("cenario_id", F.lit(cen))
                .withColumn("quarentenado_em", F.current_timestamp()).withColumn("situacao", F.lit("pendente_revisao")))
        grava(quar, T["dq_quarentena"], modo); marcas["quarentena"] = utc_now()
        if not scratch:
            marcas["quarentena_versao"], marcas["quarentena_commit"] = ultima_versao(T["dq_quarentena"])
        para_consumo = silver_t.join(sinal.select("row_id"), "row_id", "left_anti")
    else:
        para_consumo = silver_t     # execução sem verificação (módulo de tempo): não há sinalização
    marcas["gold_inicio"] = utc_now()
    gold, lin = para_gold(para_consumo)
    tg = T["gold_notif_mun_mes"] if com_gx else SCR["gold_notif_mun_mes"]
    grava(gold.withColumn("run_id", F.lit(run_id)), tg, modo)
    marcas["gold_fim"] = utc_now(); duracao = time.monotonic() - t0
    if com_gx and not scratch:
        marcas["gold_versao"], marcas["gold_commit"] = ultima_versao(tg)
        grava(lin.withColumn("run_id", F.lit(run_id)), T["gold_linhagem"])
    return run_id, duracao, marcas, ref, lote_df, silver_t

def resumo_alerta(T, run_id, cen, etapa, marcas, scratch):
    ev = spark.table(T["dq_eventos"]).where((F.col("run_id") == run_id) & (F.col("etapa") == etapa))
    cont = {r["regra_id"]: r["n"] for r in ev.groupBy("regra_id").agg(F.countDistinct("row_id").alias("n")).collect()}
    agora = utc_now()
    linhas = [(run_id, cen, etapa, k, DIMENSAO[k], int(cont.get(k, 0)), agora) for k in DIMENSAO]
    res = spark.createDataFrame(linhas, "run_id string, cenario_id string, etapa string, regra_id string, dimensao string, "
                                        "n_violacoes long, instante timestamp")
    grava(res, T["dq_resumo_regras"], "append")
    al = res.where("n_violacoes > 0").withColumn(
        "consulta_registros", F.concat(F.lit(f"SELECT * FROM {T['dq_eventos']} WHERE run_id='{run_id}' AND etapa='{etapa}' AND regra_id='"),
                                       F.col("regra_id"), F.lit("'"))) \
        .withColumn("mensagem", F.concat(F.lit("[DQ] "), F.col("dimensao"), F.lit(" em "), F.lit(etapa), F.lit(": "),
                                         F.col("n_violacoes").cast("string"), F.lit(" registro(s) sinalizado(s)")))
    if al.count() > 0:
        grava(al, T["dq_alertas"], "append")
        if not scratch:
            marcas[f"alerta_{etapa}_versao"], marcas[f"alerta_{etapa}_commit"] = ultima_versao(T["dq_alertas"])
    marcas[f"alerta_{etapa}"] = utc_now()

# COMMAND ----------
def avalia_pos(lote, run_id, ordem, marcas, ref, sel, lote_df, silver_t, duracao, destino):
    """Fora do intervalo medido: persiste referência, seleção, oráculo, aplicabilidade e manifesto."""
    cen, dim, ponto, sem, variante = lote
    if ref is not None:
        grava(ref.withColumn("run_id", F.lit(run_id)).withColumn("cenario_id", F.lit(cen)).withColumn("semente", F.lit(sem))
                 .withColumn("ponto_injecao", F.lit(ponto))
                 .withColumn("origem_defeito", F.lit("fonte (Bronze)" if ponto == "T1" else "transformação (Silver)"))
                 .withColumn("dimensao", F.lit(dim)).withColumn("regra_esperada", F.lit({v: k for k, v in DIMENSAO.items()}[dim]))
                 .withColumn("variante", F.lit(variante)), PRINC["referencia_experimental"])
        grava(sel.withColumn("run_id", F.lit(run_id)), PRINC["selecao_lotes"])
    for etapa, df in (("T1", lote_df), ("T2", silver_t.drop("violacoes_t1"))):
        grava(oraculo(df, etapa, ibge_df).withColumn("run_id", F.lit(run_id)).withColumn("etapa", F.lit(etapa)), PRINC["oraculo_violacoes"])
        ap = aplicaveis(df)
        grava(spark.createDataFrame([(run_id, etapa, k, int(v)) for k, v in ap.items()],
                                    "run_id string, etapa string, regra_id string, n_aplicaveis long"), PRINC["aplicabilidade"])
    n_t1, n_silver = lote_df.count(), silver_t.count()
    man = {"run_id": run_id, "ordem": ordem, "cenario_id": cen, "dimensao": dim, "ponto_injecao": ponto, "semente": sem,
           "variante": variante, "N": N, "m": M if dim else 0, "n_entrada_t1": n_t1, "n_silver": n_silver,
           "n_esperado": N + (M if dim == "unicidade" else 0), "versao_regras": VERSAO_REGRAS, "hash_regras": H_REGRAS,
           "hash_amostra": H_AMOSTRA, "arquivo_landing": destino, "duracao_s": round(duracao, 3), "status": "ok",
           "marcas": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in marcas.items()}}
    grava(spark.createDataFrame([(run_id, ordem, cen, json.dumps(man, default=str))],
                                "run_id string, ordem int, cenario_id string, manifesto string"), PRINC["manifesto_execucao"])

def registra_tempo(run_id, ordem, cen, cond, dur, posicao):
    grava(spark.createDataFrame([(run_id, ordem, cen, cond, float(dur), posicao, utc_now())],
                                "run_id string, ordem int, cenario_id string, condicao string, duracao_s double, "
                                "posicao_no_par int, instante timestamp"), PRINC["tempo_execucao"])

# COMMAND ----------
# Aquecimento (excluído da análise): 2 execuções por condição sobre o controle
if MODULO_TEMPO:
    dest, _, _ = entrega_fonte(("CONTROLE", None, None, 42, "principal"), "AQUECIMENTO")
    for i in range(2):
        for cond in (True, False):
            r = executa(("CONTROLE", None, None, 42, "principal"), 90 + i, dest, None, com_gx=cond, T=SCR, scratch=True)
            print("aquecimento", cond, round(r[1], 1), "s")

# COMMAND ----------
log = []
for ordem, lote in enumerate(lotes, start=1):
    cen, dim, ponto, sem, variante = lote
    run_base = f"{EXP_ID}-L{ordem:02d}-{cen}-S{sem}"
    try:
        destino, sel, ref_fonte = entrega_fonte(lote, run_base)
    except Exception:
        erro = traceback.format_exc()[-1500:]; print("FALHA na entrega", ordem, cen, erro)
        grava(spark.createDataFrame([(run_base, ordem, cen, json.dumps({"status": "falha", "etapa": "entrega", "erro": erro}))],
                                    "run_id string, ordem int, cenario_id string, manifesto string"), PRINC["manifesto_execucao"])
        continue
    condicoes = [True] if not MODULO_TEMPO else ([True, False] if ordem % 2 == 1 else [False, True])
    for pos, cond in enumerate(condicoes, start=1):
        try:
            run_id, dur, marcas, ref_slv, lote_df, silver_t = executa(lote, ordem, destino, sel, com_gx=cond)
            if MODULO_TEMPO:
                registra_tempo(run_id, ordem, cen, "com_gx" if cond else "sem_gx", dur, pos)
            if cond:
                avalia_pos(lote, run_id, ordem, marcas, ref_fonte if ref_fonte is not None else ref_slv, sel, lote_df, silver_t, dur, destino)
            log.append((ordem, cen, sem, cond, round(dur, 1), "ok"))
            print(ordem, cen, sem, "com_gx" if cond else "sem_gx", round(dur, 1), "s")
        except Exception:
            erro = traceback.format_exc()[-1500:]
            print("FALHA", ordem, cen, sem, cond, erro)
            log.append((ordem, cen, sem, cond, None, "falha"))
            if cond:
                grava(spark.createDataFrame([(run_base, ordem, cen, json.dumps({"status": "falha", "erro": erro}))],
                                            "run_id string, ordem int, cenario_id string, manifesto string"), PRINC["manifesto_execucao"])
dbutils.notebook.exit(json.dumps({"lotes": len(lotes), "falhas": sum(1 for l in log if l[-1] != "ok")}))
