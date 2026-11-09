# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Definições comuns do experimento
# MAGIC Parâmetros, regras GX congeladas (versão R-1.0), rotina de referência independente (oráculo, sem GX)
# MAGIC e funções de injeção/transformação. Importado pelos demais notebooks via `%run ./00_comum`.
# MAGIC **Experimento 2 (quarentena):** mesmo desenho do experimento 1; a diferença está em `03_execucao_lotes`: os registros
# MAGIC sinalizados em T1 ou T2 vão para `dq_quarentena` e a Gold é construída só com os registros não sinalizados.

# COMMAND ----------
import hashlib, json, datetime as dt
from pyspark.sql import functions as F, Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOG = "well_costa_classic_stable_catalog"
SCHEMA = "tcc_dq_exp2"          # experimento 2 (quarentena), isolado do experimento 1 (tcc_dq_exp)
FQ = f"{CATALOG}.{SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/raw"            # arquivos originais da fonte (somente leitura)
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"    # entrega simulada de cada lote pela fonte
RESULTADOS = f"/Volumes/{CATALOG}/{SCHEMA}/resultados"
EXP_ID = "EXP-2026-11-09"
VERSAO_REGRAS = "R-1.0"
SEMENTES = [42, 43, 44, 45, 46]
FRACAO = 0.02
SEMENTE_ORDEM = 2026          # ordem pseudoaleatória dos lotes
DIAS_INVERSAO = 7             # validade: data de início dos sintomas passa a ser 7 dias após a notificação
CODIGO_FORA_DOMINIO = "999999"  # conformidade: código municipal inexistente na malha IBGE
DATA_IMPOSSIVEL = "20241332"    # análise adicional de T1: data textual impossível

# Campos lógicos -> campos reais (Quadro 8)
MUNICIPIO = "ID_MUNICIP"        # município de notificação (6 dígitos IBGE)
DATA_EVENTO = "DT_SIN_PRI"      # data de início dos sintomas
DATA_NOTIF = "DT_NOTIFIC"       # data de notificação
CHAVE = ["ID_AGRAVO", "DT_NOTIFIC", "ID_MUNICIP", "ID_UNIDADE", "DT_SIN_PRI", "CS_SEXO", "NU_IDADE_N", "ID_MN_RESI"]
COLS_LOTE = ["row_id", "arquivo_origem", "posicao_origem", "ID_AGRAVO", "DT_NOTIFIC", "SEM_NOT", "SG_UF_NOT",
             "ID_MUNICIP", "ID_UNIDADE", "DT_SIN_PRI", "CS_SEXO", "NU_IDADE_N", "ID_MN_RESI"]

DIMENSAO = {"R_CPL": "completude", "R_CNF": "conformidade", "R_VAL": "validade", "R_UNI": "unicidade"}
ATRIBUTO = {"R_CPL": MUNICIPIO, "R_CNF": MUNICIPIO, "R_VAL": f"{DATA_EVENTO},{DATA_NOTIF}", "R_UNI": ",".join(CHAVE)}

def utc_now():
    return dt.datetime.now(dt.timezone.utc)

ARQUIVOS_FONTE = ["CHIKBR22", "CHIKBR23", "CHIKBR24"]

def extrai_dbc(arquivo):
    """Extração no próprio ambiente: .dbc (DBF comprimido com PKWare DCL) do Volume -> .dbf temporário (pyreaddbc)
    -> registros lidos campo a campo, todos como texto (latin-1). Campo em branco do DBF = ausente (None).
    Retorna (lista de dicts, nomes dos campos, total no cabeçalho)."""
    import pyreaddbc, struct, os
    dbf = f"/tmp/{arquivo}.dbf"
    pyreaddbc.dbc2dbf(f"{VOL}/{arquivo}.dbc", dbf)
    with open(dbf, "rb") as fh:
        cab = fh.read(32)
        n_reg = struct.unpack("<I", cab[4:8])[0]; tam_cab = struct.unpack("<H", cab[8:10])[0]
        tam_reg = struct.unpack("<H", cab[10:12])[0]
        campos, off = [], 0
        while True:
            d = fh.read(32)
            if not d or d[0:1] == b"\x0d":
                break
            nome = d[0:11].split(b"\x00")[0].decode("ascii", "ignore"); larg = d[16]
            campos.append((nome, off, larg)); off += larg
        fh.seek(tam_cab)
        linhas = []
        for i in range(n_reg):
            reg = fh.read(tam_reg)
            if len(reg) < tam_reg:
                break
            if reg[0:1] == b"*":
                continue
            corpo = reg[1:]
            row = {"row_id": f"{arquivo}-{i:07d}", "arquivo_origem": f"{arquivo}.dbc", "posicao_origem": i}
            for nome, o, l in campos:
                v = corpo[o:o + l].decode("latin-1", "replace").strip()
                row[nome] = v if v != "" else None
            linhas.append(row)
    os.remove(dbf)
    return linhas, [c[0] for c in campos], n_reg

def dominio_ibge():
    return sorted({r[0] for r in spark.read.text(f"{VOL}/ibge_cod6.txt")
                   .select(F.trim("value").alias("c")).where("c <> ''").collect()})

# COMMAND ----------
# MAGIC %md ## Regras (Quadro 4) em Great Expectations (GX Core, Apache-2.0) — só sinalizam, não bloqueiam

# COMMAND ----------
import great_expectations as gx
GX_CTX = gx.get_context(mode="ephemeral")
# persist=False: o GX tenta fazer cache do DataFrame por padrão, recurso indisponível em computação serverless
_BD = GX_CTX.data_sources.add_spark("spark_df", persist=False).add_dataframe_asset("lote") \
                           .add_batch_definition_whole_dataframe("lote_inteiro")
RESULT_FORMAT = {"result_format": "COMPLETE", "unexpected_index_column_names": ["row_id"], "return_unexpected_index_query": False}

def regras_gx(etapa, ibge):
    """Lista de (regra_id, expectativa). Uma regra pode ser composta por mais de uma expectativa GX;
    a ocorrência é registrada uma vez por row_id x regra. T1 = texto bruto; T2 = estado Silver tipado."""
    E = gx.expectations
    r = [("R_CPL", E.ExpectColumnValuesToNotBeNull(column=MUNICIPIO)),
         ("R_CNF", E.ExpectColumnValuesToBeInSet(column=MUNICIPIO, value_set=list(ibge))),   # nulos ignorados: ausência é R_CPL
         ("R_UNI", E.ExpectCompoundColumnsToBeUnique(column_list=CHAVE, ignore_row_if="any_value_is_missing"))]
    if etapa == "T1":
        r += [("R_VAL", E.ExpectColumnValuesToMatchStrftimeFormat(column=DATA_NOTIF, strftime_format="%Y%m%d")),
              ("R_VAL", E.ExpectColumnValuesToMatchStrftimeFormat(column=DATA_EVENTO, strftime_format="%Y%m%d")),
              ("R_VAL", E.ExpectColumnPairValuesAToBeGreaterThanB(column_A=DATA_NOTIF, column_B=DATA_EVENTO, or_equal=True,
                                                                  ignore_row_if="either_value_is_missing"))]
    else:
        r += [("R_VAL", E.ExpectColumnValuesToBeInSet(column="falha_conv_data_notificacao", value_set=[False])),
              ("R_VAL", E.ExpectColumnValuesToBeInSet(column="falha_conv_data_inicio_sintomas", value_set=[False])),
              ("R_VAL", E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="data_notificacao", column_B="data_inicio_sintomas", or_equal=True,
                                                                  ignore_row_if="either_value_is_missing"))]
    return r

def suite_gx(etapa, ibge):
    nome = f"tcc_{etapa}_{VERSAO_REGRAS}"
    try:
        GX_CTX.suites.delete(nome)
    except Exception:
        pass
    s = GX_CTX.suites.add(gx.ExpectationSuite(name=nome))
    for regra, e in regras_gx(etapa, ibge):
        e.meta = {"regra_id": regra, "dimensao": DIMENSAO[regra]}
        s.add_expectation(e)
    return s

def descreve_regras(ibge):
    out = {}
    for etapa in ("T1", "T2"):
        out[etapa] = [{"regra_id": k, **e.configuration.to_json_dict()} for k, e in regras_gx(etapa, ibge)]
        for d in out[etapa]:
            d.get("kwargs", {}).pop("value_set", None) if d["regra_id"] == "R_CNF" else None
    return out

def hash_regras(ibge):
    s = json.dumps({"versao": VERSAO_REGRAS, "regras": descreve_regras(ibge), "dominio": hashlib.sha256("\n".join(ibge).encode()).hexdigest()},
                   sort_keys=True, default=str)
    return hashlib.sha256(s.encode()).hexdigest()

def aplica_regras(df, suite):
    """Executa a suíte GX sobre o DataFrame e devolve [(row_id, regra_id, expectativa)] das linhas violadas.
    Falha de execução de qualquer expectativa interrompe o lote (não vira falso negativo)."""
    res = _BD.get_batch(batch_parameters={"dataframe": df}).validate(suite, result_format=RESULT_FORMAT)
    out = []
    for r in res.results:
        cfg = r.expectation_config
        if r.exception_info and r.exception_info.get("raised_exception"):
            raise RuntimeError(f"{cfg.type}: {r.exception_info.get('exception_message')}")
        idx = r.result.get("unexpected_index_list") or []
        assert len(idx) == (r.result.get("unexpected_count") or 0), f"lista de indices incompleta em {cfg.type}"
        out += [(u["row_id"], cfg.meta["regra_id"], cfg.type) for u in idx]
    return out

# COMMAND ----------
# MAGIC %md ## Rotina de referência independente (oráculo) — PySpark puro, sem GX e sem rótulos de injeção

# COMMAND ----------
def oraculo(df, etapa, ibge_df):
    """Retorna (row_id, regra_id) das violações segundo a especificação, sem a biblioteca de verificação.
    Aplicabilidade: R_CNF exige município não nulo; R_VAL exige as duas datas brutas presentes;
    R_UNI exige todos os atributos da chave presentes."""
    if etapa == "T1":
        de, dn = F.expr(f"try_to_date({DATA_EVENTO},'yyyyMMdd')"), F.expr(f"try_to_date({DATA_NOTIF},'yyyyMMdd')")
    else:
        de, dn = F.col("data_inicio_sintomas"), F.col("data_notificacao")
    d = (df.join(ibge_df.withColumn("_dom", F.lit(True)), F.col(MUNICIPIO) == F.col("cod6"), "left")
           .withColumn("_n", F.count(F.lit(1)).over(Window.partitionBy(*CHAVE))))
    chave_ok = F.lit(True)
    for c in CHAVE:
        chave_ok = chave_ok & F.col(c).isNotNull()
    datas_ok = F.col(DATA_EVENTO).isNotNull() & F.col(DATA_NOTIF).isNotNull()
    viol = F.array_compact(F.array(
        F.when(F.col(MUNICIPIO).isNull(), F.lit("R_CPL")),
        F.when(F.col(MUNICIPIO).isNotNull() & F.col("_dom").isNull(), F.lit("R_CNF")),
        F.when(datas_ok & (de.isNull() | dn.isNull() | (de > dn)), F.lit("R_VAL")),
        F.when(chave_ok & (F.col("_n") > 1), F.lit("R_UNI")),
    ))
    return d.select("row_id", F.explode(viol).alias("regra_id"))

def aplicaveis(df):
    """Quantidade de linhas aplicáveis por regra (denominador da matriz)."""
    chave_ok = " AND ".join(f"{c} IS NOT NULL" for c in CHAVE)
    r = df.selectExpr("count(*) n",
                      f"count_if({MUNICIPIO} IS NOT NULL) cnf",
                      f"count_if({DATA_EVENTO} IS NOT NULL AND {DATA_NOTIF} IS NOT NULL) val",
                      f"count_if({chave_ok}) uni").first()
    return {"R_CPL": r["n"], "R_CNF": r["cnf"], "R_VAL": r["val"], "R_UNI": r["uni"]}

# COMMAND ----------
# MAGIC %md ## Transformação Silver e produto Gold

# COMMAND ----------
def para_silver(df):
    """Padroniza tipos sem apagar os valores brutos: as colunas originais (texto) são mantidas
    e as datas tipadas são acrescentadas, com marca de falha de conversão."""
    return (df.withColumn("data_notificacao", F.expr(f"try_to_date({DATA_NOTIF},'yyyyMMdd')"))
              .withColumn("data_inicio_sintomas", F.expr(f"try_to_date({DATA_EVENTO},'yyyyMMdd')"))
              .withColumn("falha_conv_data_notificacao", F.col(DATA_NOTIF).isNotNull() & F.col("data_notificacao").isNull())
              .withColumn("falha_conv_data_inicio_sintomas", F.col(DATA_EVENTO).isNotNull() & F.col("data_inicio_sintomas").isNull()))

def para_gold(silver):
    """Agregação descritiva: notificações por município de notificação e mês da notificação.
    Valores ausentes/não convertíveis formam grupos próprios (não são descartados)."""
    s = silver.withColumn("mes_notificacao", F.date_trunc("month", "data_notificacao").cast("date")) \
              .withColumn("gold_key", F.concat_ws("|", F.coalesce(F.col(MUNICIPIO), F.lit("<nulo>")),
                                                  F.coalesce(F.col("mes_notificacao").cast("string"), F.lit("<nulo>"))))
    gold = s.groupBy("gold_key", F.col(MUNICIPIO).alias("municipio"), "mes_notificacao") \
            .agg(F.count(F.lit(1)).alias("n_notificacoes"))
    linhagem = s.select("gold_key", "row_id")
    return gold, linhagem

# COMMAND ----------
# MAGIC %md ## Seleção reprodutível e injeção de defeitos

# COMMAND ----------
def selecionar(df_elegivel, semente, m):
    """Ordena os elegíveis por sha2(row_id || semente) e escolhe os m primeiros (independe da ordem física)."""
    return (df_elegivel.select("row_id")
            .withColumn("_ord", F.sha2(F.concat_ws("#", "row_id", F.lit(str(semente))), 256))
            .orderBy("_ord").limit(m).select("row_id"))

def injetar(df, dimensao, ponto, sel_ids, semente, variante="principal"):
    """Aplica o defeito às linhas selecionadas. Retorna (df_alterado, df_referencia)."""
    s = sel_ids.withColumn("_sel", F.lit(True))
    d = df.join(F.broadcast(s), "row_id", "left").withColumn("_sel", F.coalesce("_sel", F.lit(False)))
    sel = F.col("_sel")
    if dimensao == "completude":
        atributo, novo = MUNICIPIO, F.lit(None).cast("string")
    elif dimensao == "conformidade":
        atributo, novo = MUNICIPIO, F.lit(CODIGO_FORA_DOMINIO)
    elif dimensao == "validade" and variante == "data_impossivel":
        atributo, novo = DATA_NOTIF, F.lit(DATA_IMPOSSIVEL)
    elif dimensao == "validade" and ponto == "T1":
        atributo = DATA_EVENTO
        novo = F.date_format(F.date_add(F.expr(f"try_to_date({DATA_NOTIF},'yyyyMMdd')"), DIAS_INVERSAO), "yyyyMMdd")
    elif dimensao == "validade" and ponto == "T2":
        atributo, novo = "data_inicio_sintomas", F.date_add("data_notificacao", DIAS_INVERSAO)
    if dimensao != "unicidade":
        ref = (d.where(sel).select("row_id", F.col(atributo).cast("string").alias("valor_antes"),
                                   novo.cast("string").alias("valor_depois"))
                 .withColumn("atributo", F.lit(atributo)).withColumn("eh_copia", F.lit(False))
                 .withColumn("row_id_original", F.col("row_id")))
        out = d.withColumn(atributo, F.when(sel, novo).otherwise(F.col(atributo))).drop("_sel")
        return out, ref
    # unicidade: acrescenta cópia com novo row_id; original e cópia são positivos (2m)
    copias = (d.where(sel).withColumn("row_id_original", F.col("row_id"))
                .withColumn("row_id", F.concat("row_id", F.lit(f"-COPIA-{semente}"))))
    ref = (copias.select("row_id", "row_id_original").withColumn("eh_copia", F.lit(True))
           .unionByName(copias.select(F.col("row_id_original").alias("row_id"), "row_id_original")
                        .withColumn("eh_copia", F.lit(False)))
           .withColumn("atributo", F.lit(",".join(CHAVE))).withColumn("valor_antes", F.lit(None).cast("string"))
           .withColumn("valor_depois", F.lit(None).cast("string")))
    out = d.drop("_sel").unionByName(copias.drop("_sel", "row_id_original"))
    return out, ref

# COMMAND ----------
def eventos(violacoes, etapa, contexto):
    """Normaliza as violações na estrutura do Quadro 5 (uma linha por row_id x regra x etapa)."""
    sch = "row_id string, regra_id string, expectativa string"
    v = spark.createDataFrame(violacoes, sch) if violacoes else spark.createDataFrame([], sch)
    mapa = F.create_map(*[x for k, d in DIMENSAO.items() for x in (F.lit(k), F.lit(d))])
    mapa_at = F.create_map(*[x for k, d in ATRIBUTO.items() for x in (F.lit(k), F.lit(d))])
    return (v.groupBy("row_id", "regra_id").agg(F.concat_ws(",", F.array_sort(F.collect_set("expectativa"))).alias("expectativas_gx"))
            .select(F.lit(contexto["run_id"]).alias("run_id"), F.lit(contexto["cenario_id"]).alias("cenario_id"),
                    F.lit(contexto["semente"]).cast("int").alias("semente"), "row_id", "regra_id",
                    F.lit(VERSAO_REGRAS).alias("versao_regra"), F.element_at(mapa, F.col("regra_id")).alias("dimensao"),
                    F.element_at(mapa_at, F.col("regra_id")).alias("atributo"), "expectativas_gx",
                    F.lit(etapa).alias("etapa"), F.lit(contexto["seq"][etapa]).alias("seq_etapa"),
                    F.lit(contexto["instante"]).cast("timestamp").alias("instante_deteccao"),
                    F.lit("violacao").alias("status"), F.lit("aviso").alias("severidade"),
                    F.lit(contexto["ref_entrada"]).alias("referencia_entrada")))

def hash_lote(df):
    """Impressão digital do lote de entrada: soma de xxhash64 das linhas (independe da ordem)."""
    r = df.select(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)")).alias("h"), F.count("*").alias("n")).first()
    return f"xx:{r['h']}:{r['n']}"

def ultima_versao(tabela):
    h = spark.sql(f"DESCRIBE HISTORY {tabela} LIMIT 1").first()
    return int(h["version"]), h["timestamp"]
