# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Preparação (antes do medalhão): extração, diagnóstico e limpeza
# MAGIC 1. **Extração**: lê os `.dbc` originais do Volume `raw` e gera os registros em texto, com `row_id` fixo.
# MAGIC 2. **Diagnóstico**: mede os defeitos naturais da base extraída (relatados à parte do experimento).
# MAGIC 3. **Limpeza / referência**: exclui registros que já violam as regras ou não se aplicam a elas; o resultado é o
# MAGIC    ponto de partida conhecido de cada lote. Nada é corrigido: registros são mantidos ou excluídos.

# COMMAND ----------
# MAGIC %run ./00_comum

# COMMAND ----------
import time, hashlib, os, importlib.metadata as md, sys
# ---- manifesto de aquisição: SHA-256 dos arquivos como estão no Volume
sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
manifesto = {"fonte": "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/", "obtido_em": "2026-09-06",
             "registrado_em_utc": utc_now().isoformat(timespec="seconds"), "arquivos": [],
             "dominio_municipal": {"arquivo": "ibge_cod6.txt", "sha256": sha(f"{VOL}/ibge_cod6.txt"),
                                   "fonte": "https://servicodados.ibge.gov.br/api/v1/localidades/municipios (2026-09-06)"}}

# ---- 1. extração dos .dbc diretamente do Volume (um arquivo por vez, gravado antes do próximo)
import pandas as pd
t0 = time.monotonic()
contagem = {}
spark.sql(f"DROP TABLE IF EXISTS {FQ}.prep_sinan_extraido")
for a in ARQUIVOS_FONTE:
    linhas, campos, n_cab = extrai_dbc(a)
    contagem[f"{a}.dbc"] = len(linhas)
    manifesto["arquivos"].append({"arquivo": f"{a}.dbc", "sha256": sha(f"{VOL}/{a}.dbc"), "bytes": os.path.getsize(f"{VOL}/{a}.dbc"),
                                  "registros_cabecalho": n_cab, "registros_lidos": len(linhas), "n_campos": len(campos),
                                  "codificacao": "latin-1"})
    pdf = pd.DataFrame.from_records(linhas, columns=["row_id", "arquivo_origem", "posicao_origem"] + campos)
    del linhas
    sch = "row_id string, arquivo_origem string, posicao_origem long, " + ", ".join(f"`{c}` string" for c in campos)
    (spark.createDataFrame(pdf, sch)
          .withColumn("sha256_origem", F.lit(manifesto["arquivos"][-1]["sha256"]))
          .withColumn("extraido_em", F.current_timestamp())
          .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{FQ}.prep_sinan_extraido"))
    del pdf
    print("extraído", a, contagem[f"{a}.dbc"])
CAMPOS_FONTE = campos
t_extr = time.monotonic() - t0
ext = spark.table(f"{FQ}.prep_sinan_extraido")
N_BRUTO = ext.count()
manifesto["n_total"] = N_BRUTO; manifesto["extracao"] = "pyreaddbc.dbc2dbf (Volume -> /tmp) + leitura DBF em Python (campos em texto)"
with open(f"{VOL}/manifesto_aquisicao.json", "w") as fh:
    json.dump(manifesto, fh, indent=2, ensure_ascii=False)
print("extraído:", N_BRUTO, contagem, "em", round(t_extr, 1), "s")

# COMMAND ----------
# MAGIC %md ## 2. Diagnóstico da base extraída (ocorrências naturais)

# COMMAND ----------
ibge = dominio_ibge()
ibge_df = spark.createDataFrame([(c,) for c in ibge], ["cod6"])
lote = ext.select(*COLS_LOTE)
t0 = time.monotonic()
viol = aplica_regras(lote, suite_gx("T1", ibge))
t_gx = time.monotonic() - t0
ev = spark.createDataFrame(viol, "row_id string, regra_id string, expectativa string").select("row_id", "regra_id").distinct()
gx_d = {r["regra_id"]: r["count"] for r in ev.groupBy("regra_id").count().collect()}
orc_rows = oraculo(lote, "T1", ibge_df)
orc_d = {r["regra_id"]: r["count"] for r in orc_rows.groupBy("regra_id").count().collect()}
so_gx = {r["regra_id"]: r["count"] for r in ev.subtract(orc_rows).groupBy("regra_id").count().collect()}
so_orc = {r["regra_id"]: r["count"] for r in orc_rows.subtract(ev).groupBy("regra_id").count().collect()}
ap = aplicaveis(lote)
diag = [(k, DIMENSAO[k], N_BRUTO, ap[k], orc_d.get(k, 0), gx_d.get(k, 0), so_gx.get(k, 0), so_orc.get(k, 0)) for k in DIMENSAO]
(spark.createDataFrame(diag, "regra_id string, dimensao string, n_registros long, n_aplicaveis long, "
                             "violacoes_oraculo long, violacoes_gx long, so_gx long, so_oraculo long")
 .withColumn("base", F.lit("extraída 2022-2024")).withColumn("gerado_em", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.diag_base_bruta"))
(orc_rows.join(lote.select("row_id", "arquivo_origem"), "row_id").groupBy("arquivo_origem", "regra_id").count()
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.diag_base_bruta_por_arquivo"))
# campos fora das regras: nulo × código "ignorado" × fora do domínio
dc = ext.selectExpr("count(*) n",
    "count_if(CS_ESCOL_N is null) escol_nulo", "count_if(CS_ESCOL_N = '9') escol_ign",
    "count_if(CS_RACA is null) raca_nulo", "count_if(CS_RACA = '9') raca_ign",
    "count_if(EVOLUCAO is null) evol_nulo", "count_if(EVOLUCAO = '9') evol_ign",
    "count_if(CLASSI_FIN is null) classi_nulo", "count_if(DT_INVEST is null) invest_nulo",
    "count_if(ANO_NASC is null) ano_nasc_nulo", "count_if(ID_UNIDADE is null) unid_nulo",
    "count_if(CS_SEXO is null) sexo_nulo", "count_if(CS_SEXO = 'I') sexo_ign",
    "count_if(ID_MN_RESI is null) resi_nulo", "count_if(DT_SIN_PRI is null) sinpri_nulo")
resi_fora = ext.where("ID_MN_RESI is not null").join(F.broadcast(ibge_df), F.col("ID_MN_RESI") == F.col("cod6"), "left_anti").count()
dc.withColumn("resi_fora", F.lit(resi_fora)).write.mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable(f"{FQ}.diag_campos_complementares")
display(spark.table(f"{FQ}.diag_base_bruta"))

# COMMAND ----------
# MAGIC %md ## 3. Limpeza: referência válida para as 4 regras (relatório de exclusões)

# COMMAND ----------
chave_ok = " AND ".join(f"{c} IS NOT NULL" for c in CHAVE)
etapas = []
d0 = ext; etapas.append(("0. base extraida", d0.count()))
d1 = d0.where(f"{MUNICIPIO} IS NOT NULL"); etapas.append(("1. municipio presente (R_CPL)", d1.count()))
d2 = d1.join(F.broadcast(ibge_df), F.col(MUNICIPIO) == F.col("cod6"), "left_semi"); etapas.append(("2. municipio no dominio IBGE (R_CNF)", d2.count()))
d3 = d2.where(f"try_to_date({DATA_EVENTO},'yyyyMMdd') IS NOT NULL AND try_to_date({DATA_NOTIF},'yyyyMMdd') IS NOT NULL "
              f"AND try_to_date({DATA_EVENTO},'yyyyMMdd') <= try_to_date({DATA_NOTIF},'yyyyMMdd')")
etapas.append(("3. datas convertiveis e inicio dos sintomas <= notificacao (R_VAL)", d3.count()))
d4 = d3.where(chave_ok); etapas.append(("4. atributos da chave presentes (aplicabilidade R_UNI)", d4.count()))
w = Window.partitionBy(*CHAVE).orderBy("row_id")
d5 = d4.withColumn("_k", F.row_number().over(w)).where("_k = 1").drop("_k")
etapas.append(("5. primeira ocorrencia de cada chave de conteudo (R_UNI)", d5.count()))
(d5.drop("sha256_origem", "extraido_em").withColumn("chave_conteudo", F.sha2(F.concat_ws("\u001f", *CHAVE), 256))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.prep_referencia"))
a = spark.table(f"{FQ}.prep_referencia")
N = a.count(); M = int(FRACAO * N)
H_AMOSTRA = hash_lote(a.select(*COLS_LOTE))
viol_amostra = oraculo(a.select(*COLS_LOTE), "T1", ibge_df).count()
dup_chave = a.groupBy("chave_conteudo").count().where("count > 1").count()
rel = spark.createDataFrame([(i, e, n) for i, (e, n) in enumerate(etapas)], "ordem int, condicao string, registros long") \
           .withColumn("excluidos", F.lag("registros").over(Window.orderBy("ordem")) - F.col("registros"))
rel.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.relatorio_exclusoes")
display(rel)

amb = {"spark": spark.version, "dbr": str(spark.sql("select current_version().dbr_version").first()[0]),
       "great_expectations": md.version("great_expectations"), "pyreaddbc": md.version("pyreaddbc"),
       "python": sys.version.split()[0],
       "delta_protocolo": spark.sql(f"DESCRIBE DETAIL {FQ}.prep_referencia").select("minReaderVersion", "minWriterVersion").first().asDict()}
param = {"exp_id": EXP_ID, "N_bruto": N_BRUTO, "N": N, "m": M, "hash_amostra": H_AMOSTRA, "violacoes_oraculo_amostra": viol_amostra,
         "chaves_repetidas_amostra": dup_chave, "n_ibge": len(ibge), "hash_regras": hash_regras(ibge), "versao_regras": VERSAO_REGRAS,
         "tempo_extracao_s": round(t_extr, 1), "tempo_gx_base_extraida_s": round(t_gx, 2), "ambiente": amb, "contagem_fonte": contagem,
         "fuso": spark.conf.get("spark.sql.session.timeZone"), "n_campos_fonte": len(CAMPOS_FONTE), "manifesto": manifesto}
spark.createDataFrame([(EXP_ID, json.dumps(param, default=str, ensure_ascii=False))], "exp_id string, parametros string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.manifesto_experimento")
print(json.dumps(param, indent=2, default=str, ensure_ascii=False))
dbutils.notebook.exit(json.dumps({"N_bruto": N_BRUTO, "N": N, "m": M}))
