# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Verificação prévia das regras (casos limítrofes) e congelamento
# MAGIC Registros sintéticos com resultado esperado declarado; GX e oráculo devem coincidir com o esperado
# MAGIC em T1 (texto) e T2 (Silver tipada). Só após 100% de concordância as regras R-1.0 são congeladas.

# COMMAND ----------
# MAGIC %run ./00_comum

# COMMAND ----------
ibge = dominio_ibge()
ibge_df = spark.createDataFrame([(c,) for c in ibge], ["cod6"])
MUN_OK = "355030"  # São Paulo (presente na malha)
base = {"arquivo_origem": "SINTETICO", "posicao_origem": 0, "ID_AGRAVO": "A92.0", "DT_NOTIFIC": "20240110", "SEM_NOT": "202402",
        "SG_UF_NOT": "35", "ID_MUNICIP": MUN_OK, "ID_UNIDADE": "1234567", "DT_SIN_PRI": "20240105", "CS_SEXO": "F",
        "NU_IDADE_N": "4030", "ID_MN_RESI": MUN_OK}
casos = [  # (row_id, alterações, esperado em T1, descrição)
    ("C01", {}, set(), "registro valido"),
    ("C02", {"ID_MUNICIP": None}, {"R_CPL"}, "municipio nulo"),
    ("C04", {"ID_MUNICIP": "999999"}, {"R_CNF"}, "codigo inexistente"),
    ("C05", {"ID_MUNICIP": "35503"}, {"R_CNF"}, "codigo com 5 digitos"),
    ("C06", {"DT_NOTIFIC": "20241332"}, {"R_VAL"}, "data impossivel"),
    ("C07", {"DT_NOTIFIC": "2024-01-10"}, {"R_VAL"}, "data em formato diferente"),
    ("C08", {"DT_SIN_PRI": "20240117"}, {"R_VAL"}, "inicio dos sintomas apos notificacao"),
    ("C09", {"DT_SIN_PRI": "20240110"}, set(), "datas iguais (limite admitido)"),
    ("C10", {"DT_SIN_PRI": None}, set(), "data do evento ausente: R_VAL nao aplicavel"),
    ("C11", {"ID_UNIDADE": "7777777"}, {"R_UNI"}, "chave repetida (1/2)"),
    ("C12", {"ID_UNIDADE": "7777777"}, {"R_UNI"}, "chave repetida (2/2)"),
    ("C13", {"ID_UNIDADE": "8888888", "CS_SEXO": None}, set(), "repeticao com atributo da chave nulo (1/2)"),
    ("C14", {"ID_UNIDADE": "8888888", "CS_SEXO": None}, set(), "repeticao com atributo da chave nulo (2/2)"),
]
# cada caso recebe valores distintos de NU_IDADE_N para não colidir na chave, salvo os pares C11-C14
rows = []
for i, (rid, alt, esp, desc) in enumerate(casos):
    r = dict(base, row_id=rid, posicao_origem=i, NU_IDADE_N=str(4000 + i) if rid not in ("C11", "C12", "C13", "C14") else "4099")
    r.update(alt); rows.append(r)
sint = spark.createDataFrame(rows, ", ".join(f"{c} {'long' if c == 'posicao_origem' else 'string'}" for c in COLS_LOTE)).select(*COLS_LOTE)

SUITES = {e: suite_gx(e, ibge) for e in ("T1", "T2")}
def obtidos(df, etapa):
    gx = {}
    for rid, regra, _ in aplica_regras(df, SUITES[etapa]):
        gx.setdefault(rid, set()).add(regra)
    orc = {}
    for r in oraculo(df, etapa, ibge_df).collect():
        orc.setdefault(r["row_id"], set()).add(r["regra_id"])
    return gx, orc

res = []
gx1, orc1 = obtidos(sint, "T1")
silver = para_silver(sint)
# caso adicional exclusivo de T2: inversão introduzida na data tipada (o texto bruto permanece válido)
inv = silver.where("row_id = 'C01'").withColumn("row_id", F.lit("C15")).withColumn("NU_IDADE_N", F.lit("4015")) \
            .withColumn("data_inicio_sintomas", F.date_add("data_notificacao", 7))
silver = silver.unionByName(inv)
gx2, orc2 = obtidos(silver, "T2")
esperado_t2 = {rid: esp for rid, _, esp, _ in casos}
esperado_t2["C15"] = {"R_VAL"}
for rid, _, esp, desc in casos + [("C15", {}, {"R_VAL"}, "inversao so na data tipada (Silver)")]:
    for etapa, dq_, orc_, e in (("T1", gx1, orc1, esp if rid != "C15" else None), ("T2", gx2, orc2, esperado_t2[rid])):
        if e is None: continue
        d_, o_ = gx1.get(rid, set()) if etapa == "T1" else gx2.get(rid, set()), orc_.get(rid, set())
        res.append((rid, desc, etapa, ",".join(sorted(e)) or "-", ",".join(sorted(d_)) or "-", ",".join(sorted(o_)) or "-",
                    d_ == e, o_ == e))
out = spark.createDataFrame(res, "caso string, descricao string, etapa string, esperado string, obtido_gx string, "
                                 "obtido_oraculo string, gx_ok boolean, oraculo_ok boolean")
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.verificacao_regras")
display(out)
falhas = out.where("NOT gx_ok OR NOT oraculo_ok").count()
congel = {"versao_regras": VERSAO_REGRAS, "hash_regras": hash_regras(ibge), "casos": out.count(), "falhas": falhas,
          "congelado": falhas == 0, "congelado_em": utc_now().isoformat(),
          "n_codigos_dominio": len(ibge), "regras": descreve_regras(ibge)}
spark.createDataFrame([(VERSAO_REGRAS, json.dumps(congel, ensure_ascii=False, default=str))], "versao string, definicao string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.regras_congeladas")
assert falhas == 0, f"{falhas} divergencias na verificacao previa — regras NAO congeladas"
dbutils.notebook.exit(json.dumps({"casos": congel["casos"], "falhas": falhas, "hash_regras": congel["hash_regras"]}))
