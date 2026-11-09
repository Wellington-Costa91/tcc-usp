# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Avaliação: matriz de classificação, métricas, precedência, rastreabilidade e tempo
# MAGIC Une a referência experimental (rótulos de injeção, guardados à parte) às ocorrências GX por
# MAGIC `run_id × row_id × regra × etapa`. Executado só depois de todos os lotes.

# COMMAND ----------
# MAGIC %run ./00_comum

# COMMAND ----------
import os
t = lambda n: spark.table(f"{FQ}.{n}")
man = t("manifesto_execucao").select("run_id", "ordem", "cenario_id", F.from_json("manifesto",
      "status string, dimensao string, ponto_injecao string, semente int, variante string, m long, n_entrada_t1 long, "
      "n_silver long, n_esperado long, duracao_s double, marcas map<string,string>").alias("x")).select("run_id", "ordem", "cenario_id", "x.*")
runs = man.where("status = 'ok'")
ev = t("dq_eventos").select("run_id", "etapa", "regra_id", "row_id").distinct()
orc = t("oraculo_violacoes").select("run_id", "etapa", "regra_id", "row_id").distinct()
ap = t("aplicabilidade")
ref = t("referencia_experimental")
etapas = spark.createDataFrame([("T1",), ("T2",)], ["etapa"])
regras = spark.createDataFrame([(k, v) for k, v in DIMENSAO.items()], ["regra_id", "dim_regra"])
# GT(regra, etapa): a regra esperada é positiva na etapa de injeção e, se injetada em T1, também em T2 (persistência)
gt = (ref.select("run_id", "row_id", F.col("regra_esperada").alias("regra_id"), "ponto_injecao").crossJoin(etapas)
         .where("etapa = ponto_injecao OR (ponto_injecao = 'T1' AND etapa = 'T2')")
         .select("run_id", "etapa", "regra_id", "row_id").distinct())

grade = runs.select("run_id", "ordem", "cenario_id", "dimensao", "ponto_injecao", "semente", "variante").crossJoin(etapas).crossJoin(regras)
def conta(df, nome):
    return df.groupBy("run_id", "etapa", "regra_id").agg(F.count("*").alias(nome))
vp = conta(gt.join(ev, ["run_id", "etapa", "regra_id", "row_id"], "left_semi"), "VP")
fn = conta(gt.join(ev, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "FN")
fp_bruto = conta(ev.join(gt, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "FP_bruto")
fp = conta(ev.join(gt, ["run_id", "etapa", "regra_id", "row_id"], "left_anti")
             .join(orc, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "FP")
so_gx = conta(ev.join(orc, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "so_gx")
so_orc = conta(orc.join(ev, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "so_oraculo")
# Efeito secundário: violação legítima criada pela injeção em outra regra (ex.: a inversão de DT_SIN_PRI,
# atributo da chave, pode tornar idênticos dois registros distintos). Identificada pelo oráculo independente,
# fora dos rótulos; não é falso positivo da regra e é relatada à parte.
sec = orc.join(gt, ["run_id", "etapa", "regra_id", "row_id"], "left_anti")
orc_fora_gt = conta(sec, "oraculo_fora_ref")
vp_sec = conta(sec.join(ev, ["run_id", "etapa", "regra_id", "row_id"], "left_semi"), "VP_secundario")
fn_sec = conta(sec.join(ev, ["run_id", "etapa", "regra_id", "row_id"], "left_anti"), "FN_secundario")
sec.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.efeitos_secundarios")
k = ["run_id", "etapa", "regra_id"]
mat = grade.join(ap, k, "left")
for d in (vp, fn, fp, fp_bruto, so_gx, so_orc, orc_fora_gt, vp_sec, fn_sec):
    mat = mat.join(d, k, "left")
mat = mat.fillna(0, ["VP", "FN", "FP", "FP_bruto", "so_gx", "so_oraculo", "oraculo_fora_ref", "VP_secundario", "FN_secundario"]) \
         .withColumn("VN", F.col("n_aplicaveis") - F.col("VP") - F.col("FN") - F.col("FP") - F.col("oraculo_fora_ref")) \
         .withColumn("papel", F.when(F.col("dimensao").isNull(), "controle")
                     .when((F.col("dim_regra") == F.col("dimensao")) & (F.col("etapa") == F.col("ponto_injecao")), "principal")
                     .when((F.col("dim_regra") == F.col("dimensao")) & (F.col("etapa") == "T2"), "persistencia")
                     .when((F.col("dim_regra") == F.col("dimensao")), "antes_da_injecao")
                     .otherwise("regra_nao_injetada"))
div = lambda a, b: F.when(b > 0, a / b)
mat = (mat.withColumn("sensibilidade", div(F.col("VP"), F.col("VP") + F.col("FN")))
          .withColumn("precisao", div(F.col("VP"), F.col("VP") + F.col("FP")))
          .withColumn("especificidade", div(F.col("VN"), F.col("VN") + F.col("FP")))
          .withColumn("f1", div(2 * F.col("VP"), 2 * F.col("VP") + F.col("FP") + F.col("FN")))
          .withColumn("taxa_fp", div(F.col("FP"), F.col("FP") + F.col("VN"))))
mat.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_matriz")
mat = t("resultado_matriz")

# COMMAND ----------
# Resumo por cenário: mediana e mínimo–máximo entre as 5 sementes + totais absolutos
def resumo(df, grupo):
    aggs = [F.sum(c).alias(f"{c}_total") for c in ("VP", "FN", "FP", "VN")] + [F.count("*").alias("lotes")]
    for c in ("sensibilidade", "precisao", "especificidade", "f1", "taxa_fp"):
        aggs += [F.percentile_approx(c, 0.5).alias(f"{c}_mediana"), F.min(c).alias(f"{c}_min"), F.max(c).alias(f"{c}_max")]
    return df.groupBy(*grupo).agg(*aggs)
res_pri = resumo(mat.where("papel = 'principal'"), ["cenario_id", "dimensao", "ponto_injecao", "regra_id", "etapa"])
res_per = resumo(mat.where("papel = 'persistencia'"), ["cenario_id", "dimensao", "ponto_injecao", "regra_id", "etapa"])
sec_res = (mat.where("oraculo_fora_ref > 0").groupBy("cenario_id", "regra_id", "etapa")
             .agg(F.count("*").alias("lotes"), F.sum("oraculo_fora_ref").alias("secundarios"), F.sum("VP_secundario").alias("detectados_gx"),
                  F.sum("FN_secundario").alias("nao_detectados"), F.min("oraculo_fora_ref").alias("min_lote"), F.max("oraculo_fora_ref").alias("max_lote")))
sec_res.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_efeitos_secundarios")
res_out = (mat.where("papel IN ('regra_nao_injetada','controle','antes_da_injecao')")
              .groupBy("papel", "regra_id", "etapa").agg(F.sum("FP").alias("FP_total"), F.sum("FP_bruto").alias("FP_bruto_total"),
                                                         F.sum("oraculo_fora_ref").alias("secundarios_total"),
                                                         F.sum("VP_secundario").alias("VP_secundario_total"), F.sum("VN").alias("VN_total"),
                                                         F.count("*").alias("pares_lote_regra")))
for nome, df in (("resultado_principal", res_pri), ("resultado_persistencia", res_per), ("resultado_outras_regras", res_out)):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.{nome}")
display(t("resultado_principal").orderBy("ponto_injecao", "dimensao"))

# COMMAND ----------
# Detecção antes da Gold e precedência das sinalizações
m = runs.select("run_id", "cenario_id", "dimensao", "ponto_injecao",
                F.to_timestamp(F.col("marcas")["gold_inicio"]).alias("gold_inicio"),
                F.to_timestamp(F.col("marcas")["gold_commit"]).alias("gold_commit"),
                F.coalesce(F.to_timestamp(F.col("marcas")["alerta_T2_commit"]), F.to_timestamp(F.col("marcas")["alerta_T1_commit"])).alias("ultimo_alerta_commit"),
                F.to_timestamp(F.col("marcas")["quarentena_commit"]).alias("quarentena_commit"),
                F.col("marcas")["gold_versao"].cast("long").alias("gold_versao"))
ult_ev = t("dq_eventos").groupBy("run_id").agg(F.max("instante_deteccao").alias("ultima_deteccao"), F.count("*").alias("eventos"))
pos_unicos = ref.select("run_id", "row_id", F.col("regra_esperada").alias("regra_id")).distinct()
det_antes = (pos_unicos.join(ev.select("run_id", "regra_id", "row_id").distinct(), ["run_id", "regra_id", "row_id"], "left_semi")
             .groupBy("run_id").agg(F.count("*").alias("positivos_detectados")))
tot_pos = pos_unicos.groupBy("run_id").agg(F.count("*").alias("positivos"))
prec = (m.join(ult_ev, "run_id", "left").join(tot_pos, "run_id", "left").join(det_antes, "run_id", "left")
         .withColumn("pct_detectado_antes_gold", div(F.col("positivos_detectados"), F.col("positivos")))
         .withColumn("deteccao_antes_gold", F.col("ultima_deteccao") < F.col("gold_inicio"))
         .withColumn("alerta_commit_antes_gold_commit", F.col("ultimo_alerta_commit") < F.col("gold_commit"))
         .withColumn("quarentena_commit_antes_gold_commit", F.col("quarentena_commit") < F.col("gold_commit")))
prec.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_precedencia")

# COMMAND ----------
# Rastreabilidade: campos obrigatórios em cada ocorrência + vínculo da ocorrência T2 a um agregado Gold
e = t("dq_eventos")
obrig = ["run_id", "cenario_id", "row_id", "regra_id", "versao_regra", "dimensao", "atributo", "etapa", "instante_deteccao", "severidade", "referencia_entrada"]
completo = F.lit(True)
for c in obrig:
    completo = completo & F.col(c).isNotNull() & (F.col(c).cast("string") != "")
rast = e.groupBy("run_id").agg(F.count("*").alias("ocorrencias"), F.count_if(completo).alias("ocorrencias_completas"))
lin = t("gold_linhagem")
e2 = e.where("etapa = 'T2'").select("run_id", "row_id").distinct()
lig = (e2.join(lin.select("run_id", "row_id").distinct(), ["run_id", "row_id"], "left_semi").groupBy("run_id").agg(F.count("*").alias("t2_com_gold")))
e2n = e2.groupBy("run_id").agg(F.count("*").alias("t2_sinalizados"))
gsum = t("gold_notif_mun_mes").groupBy("run_id").agg(F.sum("n_notificacoes").alias("soma_gold"))
lsum = lin.groupBy("run_id").agg(F.countDistinct("row_id").alias("row_ids_linhagem"))
rast = (runs.select("run_id", "cenario_id", "n_entrada_t1", "n_silver", "n_esperado").join(rast, "run_id", "left")
        .join(e2n, "run_id", "left").join(lig, "run_id", "left").join(gsum, "run_id", "left").join(lsum, "run_id", "left")
        .withColumn("cobertura_campos", div(F.col("ocorrencias_completas"), F.col("ocorrencias")))
        .withColumn("cobertura_linhagem_gold", div(F.col("t2_com_gold"), F.col("t2_sinalizados")))
        .join(t("dq_quarentena").groupBy("run_id").agg(F.countDistinct("row_id").alias("n_quarentena")), "run_id", "left")
        .fillna(0, ["n_quarentena"])
        # experimento 2: a Gold recebe só os não sinalizados; Silver = Gold + quarentena
        .withColumn("cardinalidade_ok", (F.col("n_silver") == F.col("n_esperado"))
                    & (F.col("soma_gold") + F.col("n_quarentena") == F.col("n_silver"))
                    & (F.col("row_ids_linhagem") + F.col("n_quarentena") == F.col("n_silver"))))
rast.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_rastreabilidade")

# COMMAND ----------
# Experimento 2 · quarentena: (a) a quarentena contém exatamente os registros sinalizados; (b) a Gold não contém nenhum
# registro sinalizado; (c) quarentena + Gold = Silver; (d) a quarentena coincide com a rotina de referência independente;
# (e) a Gold é idêntica à Gold construída a partir da base de referência sem os registros quarentenados.
qr = t("dq_quarentena").select("run_id", "row_id").distinct()
sinal_ids = ev.select("run_id", "row_id").distinct()
orc_ids = orc.select("run_id", "row_id").distinct()
cnt = lambda df, nome: df.groupBy("run_id").agg(F.count("*").alias(nome))
q_n = cnt(qr, "n_quarentena")
s_n = cnt(sinal_ids, "n_sinalizados")
q_fora = cnt(qr.join(sinal_ids, ["run_id", "row_id"], "left_anti"), "quarentena_sem_sinal")
s_fora = cnt(sinal_ids.join(qr, ["run_id", "row_id"], "left_anti"), "sinal_fora_quarentena")
g_sinal = cnt(lin.select("run_id", "row_id").distinct().join(sinal_ids, ["run_id", "row_id"], "left_semi"), "sinalizados_na_gold")
q_orc = cnt(qr.join(orc_ids, ["run_id", "row_id"], "left_anti"), "quarentena_fora_oraculo")
orc_q = cnt(orc_ids.join(qr, ["run_id", "row_id"], "left_anti"), "oraculo_fora_quarentena")
referencia = t("prep_referencia").drop("chave_conteudo")
eq = []
for r in runs.select("run_id").collect():
    rid = r["run_id"]
    esperado, _ = para_gold(para_silver(referencia.join(qr.where(F.col("run_id") == rid).select("row_id"), "row_id", "left_anti")))
    obtido = t("gold_notif_mun_mes").where(F.col("run_id") == rid).drop("run_id")
    a = esperado.select("gold_key", "n_notificacoes"); b = obtido.select("gold_key", "n_notificacoes")
    eq.append((rid, a.exceptAll(b).count() + b.exceptAll(a).count()))
g_eq = spark.createDataFrame(eq, "run_id string, diferencas_gold_vs_referencia long")
qres = runs.select("run_id", "ordem", "cenario_id", "n_silver")
for d in (q_n, s_n, q_fora, s_fora, g_sinal, q_orc, orc_q, g_eq):
    qres = qres.join(d, "run_id", "left")
qres = (qres.fillna(0, ["n_quarentena", "n_sinalizados", "quarentena_sem_sinal", "sinal_fora_quarentena", "sinalizados_na_gold",
                        "quarentena_fora_oraculo", "oraculo_fora_quarentena"])
        .join(lsum, "run_id", "left")
        .withColumn("quarentena_igual_sinalizados", (F.col("quarentena_sem_sinal") == 0) & (F.col("sinal_fora_quarentena") == 0))
        .withColumn("gold_sem_sinalizados", F.col("sinalizados_na_gold") == 0)
        .withColumn("quarentena_mais_gold_igual_silver", F.col("n_quarentena") + F.col("row_ids_linhagem") == F.col("n_silver"))
        .withColumn("quarentena_igual_oraculo", (F.col("quarentena_fora_oraculo") == 0) & (F.col("oraculo_fora_quarentena") == 0))
        .withColumn("gold_igual_referencia", F.col("diferencas_gold_vs_referencia") == 0))
qres.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_quarentena")
display(t("resultado_quarentena").orderBy("ordem"))

# COMMAND ----------
# Módulo de tempo (exploratório): pares com e sem GX
if spark.catalog.tableExists(f"{FQ}.tempo_execucao"):
    tp = t("tempo_execucao").groupBy("ordem", "cenario_id").pivot("condicao", ["com_gx", "sem_gx"]).agg(F.first("duracao_s"))
    tp = tp.withColumn("acrescimo_s", F.col("com_gx") - F.col("sem_gx")) \
           .withColumn("acrescimo_pct", 100 * (F.col("com_gx") - F.col("sem_gx")) / F.col("sem_gx"))
    tp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.resultado_tempo")

# COMMAND ----------
# Exporta as tabelas de resultado em CSV para o material suplementar
out = RESULTADOS; os.makedirs(out, exist_ok=True)
for n in ["diag_base_bruta", "diag_base_bruta_por_arquivo", "diag_campos_complementares", "relatorio_exclusoes", "verificacao_regras", "manifesto_experimento",
          "regras_congeladas", "manifesto_execucao", "resultado_matriz", "resultado_principal", "resultado_persistencia",
          "resultado_outras_regras", "resultado_efeitos_secundarios", "resultado_precedencia", "resultado_rastreabilidade", "resultado_tempo", "dq_resumo_regras", "dq_alertas",
          "resultado_quarentena", "aplicabilidade", "efeitos_secundarios"]:
    if spark.catalog.tableExists(f"{FQ}.{n}"):
        t(n).toPandas().to_csv(f"{out}/{n}.csv", index=False)
# amostras pequenas (evidência de estrutura)
t("dq_eventos").limit(200).toPandas().to_csv(f"{out}/amostra_dq_eventos.csv", index=False)
t("referencia_experimental").limit(200).toPandas().to_csv(f"{out}/amostra_referencia_experimental.csv", index=False)
t("gold_notif_mun_mes").limit(200).toPandas().to_csv(f"{out}/amostra_gold.csv", index=False)
t("dq_quarentena").limit(200).toPandas().to_csv(f"{out}/amostra_dq_quarentena.csv", index=False)
dbutils.notebook.exit("ok")
