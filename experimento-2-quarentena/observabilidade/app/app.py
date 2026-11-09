"""Observabilidade do EXPERIMENTO 2 do TCC (quarentena), schema tcc_dq_exp2.
Igual ao app do experimento 1, com a prova da quarentena na Pergunta 4.

Cada seção responde a uma pergunta do orientador, na ordem em que um leitor externo precisa delas:
  1. O pipeline executou?                       -> linha do tempo das tarefas do job
  2. Os defeitos foram detectados? Houve FP?   -> matriz cenário × regra/ponto
  3. O alerta chega antes do consumo? Parou?    -> linha do tempo de cada um dos 50 lotes
  4. O que chegou à Gold?                       -> grupos da Gold comprometidos (via linhagem)
  5. Quanto custa a observabilidade?            -> composição do tempo do lote, com × sem
  6. Que metadados são gerados?                 -> amostra das tabelas de alertas e eventos
Gráficos sem título interno (o título vai na legenda da figura no texto). O botão de câmera de cada
gráfico baixa um PNG em alta resolução."""
import json
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from databricks.sdk import WorkspaceClient

S = os.environ.get("SCHEMA", "well_costa_classic_stable_catalog.tcc_dq_exp2")
EXP = "EXP-2026-11-09"
VERDE_Q = "#1baf7a"
st.set_page_config(page_title="Observabilidade · Qualidade de dados", page_icon="📊", layout="wide")

# Paleta (referência validada da skill de dataviz): azul = verificação, laranja = alerta, cinzas = pipeline.
AZUL, LARANJA, VERDE = "#2a78d6", "#eb6834", "#1baf7a"
CINZA_CL, CINZA, GRAFITE, TEXTO, TEXTO2 = "#d5d8dc", "#9aa1a9", "#3b3f45", "#0b0b0b", "#52514e"
DIM = {"COM": "Completude", "CON": "Conformidade", "VAL": "Validade", "VALIMP": "Validade (data impossível)",
       "UNI": "Unicidade", "CONTROLE": "Controle (sem defeito)"}
ORIG = {"BRZ": "na fonte", "SLV": "na transformação"}
ORDEM_CEN = ["COM-BRZ", "COM-SLV", "CON-BRZ", "CON-SLV", "VAL-BRZ", "VAL-SLV", "VALIMP-BRZ", "UNI-BRZ", "UNI-SLV", "CONTROLE"]

st.markdown(f"""
<style>
.block-container {{max-width: 1180px; padding-top: 1.4rem;}}
.hero {{background:#fff; border:1px solid #E3E6EA; border-left:6px solid {AZUL}; padding:18px 24px; border-radius:12px; margin-bottom:16px;}}
.hero h1 {{font-size:1.65rem; margin:0 0 4px 0; color:{TEXTO};}}
.hero p {{color:{TEXTO2}; margin:0; font-size:.95rem;}}
.kpi {{background:#fff; border:1px solid #E3E6EA; border-radius:12px; padding:14px 16px; height:100%;}}
.kpi .v {{font-size:1.65rem; font-weight:700; color:{TEXTO}; line-height:1.1;}}
.kpi .l {{font-size:.82rem; color:{TEXTO2}; margin-top:4px;}}
.q {{font-size:.8rem; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:{AZUL}; margin:28px 0 0 0;}}
.nota {{color:{TEXTO2}; font-size:.92rem; margin:2px 0 8px 0; text-align:justify;}}
.leitura {{background:#F5F8FC; border-radius:10px; padding:10px 14px; color:{TEXTO}; font-size:.92rem; margin:4px 0 12px 0;}}
</style>""", unsafe_allow_html=True)

w = WorkspaceClient()


@st.cache_data(ttl=3600, show_spinner=False)
def q(sql):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=os.environ["WAREHOUSE_ID"], wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(r.status.error)
    return pd.DataFrame(r.result.data_array or [], columns=[c.name for c in r.manifest.schema.columns])


def rotulo(cid):
    if cid == "CONTROLE":
        return DIM[cid]
    d, o = cid.rsplit("-", 1)
    return f"{DIM[d]} · {ORIG[o]}"


def br(v, casas=0):
    s = f"{v:,.{casas}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


def estilo(fig, h, legenda=True):
    fig.update_layout(template="simple_white", title_text="", height=h, separators=",.",
                      font=dict(family="Arial", size=14, color=TEXTO), paper_bgcolor="white", plot_bgcolor="white",
                      margin=dict(l=10, r=30, t=40, b=10), showlegend=legenda, legend_title_text="",
                      legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0, font=dict(size=13)),
                      hoverlabel=dict(font_size=13, bgcolor="white"))
    fig.update_xaxes(linecolor=CINZA, tickcolor=CINZA, title_font=dict(color=TEXTO2))
    fig.update_yaxes(linecolor=CINZA, tickcolor=CINZA, title_font=dict(color=TEXTO2))
    return fig


def mostra(fig, nome):
    st.plotly_chart(fig, use_container_width=True, config={
        "displaylogo": False, "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d", "zoom2d", "pan2d"],
        "toImageButtonOptions": {"format": "png", "filename": nome, "scale": 3, "width": 1100, "height": fig.layout.height}})


def secao(n, pergunta, titulo, nota):
    st.markdown(f'<p class="q">Pergunta {n} · {pergunta}</p>', unsafe_allow_html=True)
    st.subheader(titulo)
    st.markdown(f'<p class="nota">{nota}</p>', unsafe_allow_html=True)


def leitura(txt):
    st.markdown(f'<div class="leitura">🔎 <b>Como ler:</b> {txt}</div>', unsafe_allow_html=True)


def kpi(col, valor, legenda):
    col.markdown(f'<div class="kpi"><div class="v">{valor}</div><div class="l">{legenda}</div></div>', unsafe_allow_html=True)


def tabela(df, cab, nome, larguras):
    fig = go.Figure(go.Table(columnwidth=larguras,
        header=dict(values=[f"<b>{c}</b>" for c in cab], fill_color=GRAFITE, font=dict(color="white", size=13), align="left", height=34),
        cells=dict(values=[df[c] for c in df.columns], fill_color=[["#FFFFFF", "#F5F6F7"] * len(df)], align="left",
                   line_color="#E3E6EA", font=dict(size=12.5, color=TEXTO), height=30)))
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=34 + 30 * len(df) + 14)
    mostra(fig, nome)



# ------------------------------------------------------------------ dados
with st.spinner("Carregando os resultados do experimento…"):
    tarefas = q(f"select task_key, inicio, fim from {S}.obs_job_tarefas order by inicio")
    tempo = q(f"select cenario_id, condicao, duracao_s from {S}.tempo_execucao")
    matriz = q(f"""select cenario_id, regra_id, etapa, sum(cast(n_violacoes as bigint)) n
                   from {S}.dq_resumo_regras group by all""")
    manif = q(f"select cast(ordem as int) ordem, cenario_id, manifesto from {S}.manifesto_execucao")
    qres = q(f"""select cenario_id, n_sinalizados, n_quarentena, sinalizados_na_gold, row_ids_linhagem, n_silver,
                 cast(quarentena_igual_sinalizados as string) c1, cast(gold_sem_sinalizados as string) c2,
                 cast(quarentena_mais_gold_igual_silver as string) c3, cast(quarentena_igual_oraculo as string) c4,
                 cast(gold_igual_referencia as string) c5 from {S}.resultado_quarentena""")
    quar = q(f"""select row_id, array_join(violacoes, ', ') violacoes, DT_NOTIFIC, ID_MUNICIP,
                 date_format(quarentenado_em,'HH:mm:ss') em, situacao
                 from {S}.dq_quarentena where run_id = '{EXP}-L14-VALIMP-BRZ-S43' order by row_id limit 8""")
    alertas = q(f"""select cenario_id, etapa, dimensao, n_violacoes, date_format(instante,'HH:mm:ss') instante
                    from {S}.dq_alertas order by instante limit 8""")
    eventos = q(f"""select row_id, regra_id, dimensao, atributo, etapa, date_format(instante_deteccao,'HH:mm:ss') instante, status
                    from {S}.dq_eventos where run_id = '{EXP}-L02-UNI-BRZ-S45' order by row_id, etapa limit 8""")

tempo["duracao_s"] = tempo.duracao_s.astype(float)
matriz["n"] = matriz.n.astype(int)
for c in ["n_sinalizados", "n_quarentena", "sinalizados_na_gold", "row_ids_linhagem", "n_silver"]:
    qres[c] = qres[c].astype(int)
med = tempo.groupby("condicao").duracao_s.median()

# marcas de tempo de cada lote (gravadas pelo pipeline no manifesto)
T0 = lambda m, k: pd.Timestamp(m[k]) if m.get(k) else None
lotes = []
for _, r in manif.iterrows():
    mk = json.loads(r.manifesto)["marcas"]
    t0 = pd.Timestamp(mk["inicio"])
    s = lambda k: (pd.Timestamp(mk[k]) - t0).total_seconds() if mk.get(k) else None
    lotes.append(dict(ordem=int(r.ordem), cenario_id=r.cenario_id, bronze=s("bronze"), t1=s("t1_eventos"), a1=s("alerta_T1"),
                      silver=s("silver"), t2=s("t2_eventos"), a2=s("alerta_T2"), qz=s("quarentena"), g0=s("gold_inicio"), g1=s("gold_fim"),
                      alerta_t1=mk.get("alerta_T1_versao") is not None, alerta_t2=mk.get("alerta_T2_versao") is not None))
lotes = pd.DataFrame(lotes).sort_values("ordem")
com_def = lotes[lotes.cenario_id != "CONTROLE"]
antes = int(((com_def.alerta_t1 | com_def.alerta_t2) & (com_def[["a1", "a2"]].max(axis=1) <= com_def.g0)).sum())
q_antes = int((lotes.qz <= lotes.g0).sum())

# ------------------------------------------------------------------ cabeçalho
st.markdown("""<div class="hero"><h1>Experimento 2 · observabilidade com quarentena</h1>
<p>Execução de 2026-11-09 · 50 lotes do SINAN/Chikungunya (2022–2024) · fluxo Bronze → T1 → Silver → T2 → Gold ·
verificação com Great Expectations · registros sinalizados vão para a quarentena e a Gold recebe só os demais</p></div>""", unsafe_allow_html=True)
k = st.columns(6)
kpi(k[0], "50 de 50", "lotes executados sem falha")
esperadas = matriz[matriz.apply(lambda r: r.cenario_id != "CONTROLE" and r.regra_id == {"COM": "R_CPL", "CON": "R_CNF", "VAL": "R_VAL", "VALIMP": "R_VAL", "UNI": "R_UNI"}[r.cenario_id.rsplit("-", 1)[0]], axis=1)]
kpi(k[1], "100%", "dos registros com defeito sinalizados pela regra esperada")
kpi(k[2], "0", "registros válidos sinalizados indevidamente (falsos positivos)")
kpi(k[3], f"{antes} de {len(com_def)}", "lotes com defeito: alerta gravado antes da Gold")
kpi(k[4], br(int(qres.sinalizados_na_gold.sum())), "registros sinalizados que chegaram à Gold")
kpi(k[5], f"{br(med['com_gx'], 0)} s × {br(med['sem_gx'], 0)} s", "duração mediana do lote com × sem verificação")

# ------------------------------------------------------------------ 1. execução
secao(1, "O pipeline executou?", "Execução do job no Databricks",
      "Linha do tempo das quatro tarefas do job (dados da API de Jobs do Databricks). A tarefa 3 executa os 50 lotes em série, "
      "cada um duas vezes: com e sem as verificações de qualidade.")
nomes = {"t01_preparacao": "1 · Preparação da base", "t02_verificacao_regras": "2 · Verificação das regras",
         "t03_execucao_lotes": "3 · Execução dos 50 lotes", "t04_avaliacao": "4 · Avaliação dos resultados"}
tarefas["inicio"], tarefas["fim"] = pd.to_datetime(tarefas.inicio), pd.to_datetime(tarefas.fim)
fig = go.Figure()
for _, r in tarefas.iterrows():
    dur = (r.fim - r.inicio).total_seconds() / 60
    fig.add_bar(y=[nomes[r.task_key]], x=[(r.fim - r.inicio).total_seconds() * 1000], base=[r.inicio], orientation="h",
                marker_color=VERDE, showlegend=False, hovertemplate=f"{nomes[r.task_key]}<br>{br(dur, 1)} min · concluída<extra></extra>")
    fig.add_annotation(y=nomes[r.task_key], x=r.fim, text=f"  {br(dur, 0) if dur >= 1 else '<1'} min ✓", showarrow=False,
                       xanchor="left", font=dict(color=TEXTO2, size=13))
fig.update_xaxes(type="date", tickformat="%H:%M", title="Horário (UTC) em 2026-11-09",
                 range=[tarefas.inicio.min() - pd.Timedelta(minutes=3), tarefas.fim.max() + pd.Timedelta(minutes=12)])
fig.update_yaxes(autorange="reversed", title=None)
fig.update_layout(bargap=0.45)
mostra(estilo(fig, 280, legenda=False), "fig-1-execucao-job")
_tot = (tarefas.fim.max() - tarefas.inicio.min()).total_seconds() / 60
leitura(f"cada barra é uma tarefa do job; o ✓ indica conclusão com sucesso. As 4 tarefas terminaram sem erro em {int(_tot // 60)}h{int(_tot % 60):02d}.")

# ------------------------------------------------------------------ 2. matriz de detecção
secao(2, "Os defeitos foram detectados? Houve falso positivo?", "Onde cada defeito foi sinalizado",
      "Cada linha é um cenário (dimensão do defeito e onde ele foi introduzido); cada coluna, uma regra em um ponto de "
      "verificação. A célula mostra a fração dos registros com defeito que a regra sinalizou, somando os 5 lotes do cenário.")
REGRAS = [("R_CPL", "Completude"), ("R_CNF", "Conformidade"), ("R_VAL", "Validade"), ("R_UNI", "Unicidade")]
cols = [(rg, et) for rg, _ in REGRAS for et in ("T1", "T2")]
xlab = [f"{nm}<br><b>{et}</b>" for _, nm in REGRAS for et in ("T1", "T2")]
z, txt = [], []
for cid in ORDEM_CEN:
    linha, tl = [], []
    base = 2 * 95090 if cid.startswith("UNI") else 95090   # unicidade: original + cópia são positivos
    for rg, et in cols:
        n = matriz[(matriz.cenario_id == cid) & (matriz.regra_id == rg) & (matriz.etapa == et)].n.sum()
        if n == 0:
            linha.append(0); tl.append("")
        elif n / base >= 0.995:
            linha.append(100); tl.append("<b>100%</b>")
        else:
            linha.append(max(4, 100 * n / base)); tl.append(f"{br(n)} reg.¹")
    z.append(linha); txt.append(tl)
fig = go.Figure(go.Heatmap(z=z, x=xlab, y=[rotulo(c) for c in ORDEM_CEN], text=txt, texttemplate="%{text}",
                           textfont=dict(size=13), colorscale=[[0, "#FFFFFF"], [0.04, "#DCE9F8"], [1, AZUL]], zmin=0, zmax=100,
                           xgap=3, ygap=3, showscale=False, hoverinfo="skip"))
fig.update_traces(textfont_color=TEXTO)
for i, cid in enumerate(ORDEM_CEN):     # texto branco nas células cheias
    for j, v in enumerate(z[i]):
        if v == 100:
            fig.add_annotation(x=xlab[j], y=rotulo(cid), text="<b>100%</b>", showarrow=False, font=dict(color="white", size=13))
fig.update_traces(text=[[t if "100%" not in t else "" for t in row] for row in txt])
fig.update_yaxes(autorange="reversed", showline=False, ticks="")
fig.update_xaxes(side="top", showline=False, ticks="")
for xpos in (1.5, 3.5, 5.5):
    fig.add_vline(x=xpos, line_color=CINZA, line_width=1)
mostra(estilo(fig, 520, legenda=False), "fig-2-matriz-deteccao")
leitura("cada cenário acende apenas a coluna da sua dimensão. Defeito introduzido na fonte aparece em T1 e continua "
        "visível em T2; defeito introduzido na transformação aparece só em T2. A linha de controle fica vazia: nenhum "
        "registro válido foi sinalizado. ¹ Duplicatas reais criadas pela própria injeção de validade, confirmadas pela "
        "rotina de referência independente (não são falsos positivos).")

# ------------------------------------------------------------------ 3. linha do tempo dos lotes
secao(3, "O alerta chega antes do consumo? O pipeline parou?", "Linha do tempo de cada lote",
      "Cada linha é um dos 50 lotes, com o tempo contado a partir do seu início. As barras azuis são as verificações de "
      "qualidade; os losangos laranja, os alertas gravados; o triângulo verde, a gravação da quarentena; a barra escura, a construção da Gold (camada de consumo). Todos os instantes vêm do relógio do controlador.")
etapas = [("Ingestão Bronze", 0, "bronze", CINZA_CL), ("Verificação T1", "bronze", "t1", AZUL), ("Silver", "t1", "silver", CINZA),
          ("Verificação T2", "silver", "t2", AZUL), ("Construção da Gold", "g0", "g1", GRAFITE)]
lotes["y"] = lotes.apply(lambda r: f"L{r.ordem:02d} · {rotulo(r.cenario_id)}", axis=1)
fig = go.Figure()
for nome, a, b, cor in etapas:
    ini = lotes[a] if isinstance(a, str) else 0
    fig.add_bar(y=lotes.y, x=lotes[b] - ini, base=ini, orientation="h", name=nome, marker_color=cor,
                legendgroup=nome, showlegend=nome != "Verificação T2",
                hovertemplate="%{y}<br>" + nome + ": %{x:.1f} s<extra></extra>")
fig.data[1].name = "Verificação (T1 e T2)"
al = pd.concat([lotes[lotes.alerta_t1][["y", "a1"]].rename(columns={"a1": "t"}), lotes[lotes.alerta_t2][["y", "a2"]].rename(columns={"a2": "t"})])
fig.add_scatter(y=al.y, x=al.t, mode="markers", name="Alerta gravado", marker=dict(symbol="diamond", size=11, color=LARANJA, line=dict(color="white", width=1.5)),
                hovertemplate="%{y}<br>alerta em %{x:.1f} s<extra></extra>")
fig.add_scatter(y=lotes.y, x=lotes.qz, mode="markers", name="Quarentena gravada", marker=dict(symbol="triangle-right", size=10, color=VERDE_Q,
                line=dict(color="white", width=1)), hovertemplate="%{y}<br>quarentena em %{x:.1f} s<extra></extra>")
fig.update_layout(barmode="overlay", bargap=0.35)
fig.update_yaxes(autorange="reversed", title=None, tickfont=dict(size=11))
fig.update_xaxes(title="Segundos desde o início do lote", showgrid=True, gridcolor="#F0F1F3", dtick=10)
mostra(estilo(fig, 1180), "fig-3-linha-do-tempo-lotes")
leitura(f"em todos os {len(com_def)} lotes com defeito o último losango laranja (alerta) fica à esquerda da barra escura "
        "(construção da Gold), e o triângulo verde (quarentena) também: os registros sinalizados são isolados antes de a camada de "
        f"consumo ser construída ({q_antes} de {len(lotes)} lotes). O pipeline <b>não para</b>: segue com os registros limpos.")

# ------------------------------------------------------------------ 4. quarentena × Gold
secao(4, "Algum dado com defeito chegou à Gold?", "Registros sinalizados, em quarentena e na Gold",
      "Antes de construir a Gold, todo registro sinalizado em T1 ou T2 é gravado na tabela de quarentena (<i>dq_quarentena</i>), "
      "com as regras violadas, e fica pendente de revisão. A Gold recebe apenas os registros não sinalizados.")
qres["dim"] = qres.cenario_id.map(lambda c: DIM[c.rsplit("-", 1)[0]] if c != "CONTROLE" else DIM[c])
qd = qres.groupby("dim", as_index=False)[["n_sinalizados", "n_quarentena", "sinalizados_na_gold"]].sum().sort_values("n_sinalizados")
fig = go.Figure()
fig.add_bar(y=qd.dim, x=qd.n_sinalizados.astype(float), name="sinalizados (T1 ou T2)", orientation="h", marker_color=LARANJA)
fig.add_bar(y=qd.dim, x=qd.n_quarentena.astype(float), name="em quarentena", orientation="h", marker_color=VERDE_Q)
for _, r in qd.iterrows():
    fig.add_annotation(y=r.dim, x=max(r.n_sinalizados, r.n_quarentena), text=f"  na Gold: <b>{br(int(r.sinalizados_na_gold))}</b>",
                       showarrow=False, xanchor="left", font=dict(size=13, color=TEXTO))
fig.update_layout(barmode="group", bargap=0.3)
fig.update_xaxes(title="Registros (soma dos 5 lotes de cada dimensão)", tickformat=",.0f", range=[0, qd.n_sinalizados.max() * 1.3],
                 showgrid=True, gridcolor="#F0F1F3")
fig.update_yaxes(title=None)
mostra(estilo(fig, 400), "fig-4-quarentena-gold")
leitura("as barras laranja (sinalizados) e verdes (em quarentena) têm o mesmo tamanho em todas as dimensões, e nenhum registro "
        "sinalizado chegou à Gold. No controle não há sinalização nem quarentena.")
st.markdown("**Conferências da quarentena por lote**")
crit = [("c1", "A quarentena contém exatamente os registros sinalizados"), ("c2", "A Gold não contém nenhum registro sinalizado"),
        ("c3", "Quarentena + Gold = todos os registros da Silver"), ("c4", "A quarentena coincide com a rotina de referência independente"),
        ("c5", "A Gold é idêntica à construída a partir da base de referência sem os registros em quarentena")]
conf = pd.DataFrame([(nome, f"{(qres[c] == 'true').sum()} de {len(qres)}") for c, nome in crit], columns=["criterio", "lotes"])
tabela(conf, ["Critério", "Lotes que atendem"], "fig-4b-conferencias-quarentena", [6, 1.4])

# ------------------------------------------------------------------ 5. custo
secao(5, "Quanto custa a observabilidade?", "Composição do tempo de um lote",
      "Tempo mediano de cada etapa nos 50 lotes executados com verificação, comparado ao tempo total do mesmo lote "
      "executado sem verificação.")
etp = pd.DataFrame({"Ingestão Bronze": lotes.bronze, "Verificação T1": lotes.t1 - lotes.bronze,
                    "Alertas e Silver": lotes.silver - lotes.t1, "Verificação T2": lotes.t2 - lotes.silver,
                    "Alerta T2 e Gold": lotes.g1 - lotes.t2}).median()
cores = {"Ingestão Bronze": CINZA_CL, "Verificação T1": AZUL, "Alertas e Silver": CINZA, "Verificação T2": AZUL, "Alerta T2 e Gold": GRAFITE}
Y = ["Com verificações", "Sem verificações"]
fig = go.Figure()
for nome, v in etp.items():
    fig.add_bar(y=[Y[0]], x=[v], name=nome, orientation="h", marker_color=cores[nome], marker_line=dict(color="white", width=2),
                text=[f"{br(v, 0)} s" if v >= 4 else ""], textposition="inside", insidetextanchor="middle",
                textfont=dict(color="white" if cores[nome] in (AZUL, GRAFITE, CINZA) else TEXTO),
                showlegend=nome != "Verificação T2", legendgroup=nome,
                hovertemplate=nome + ": %{x:.1f} s<extra></extra>")
fig.data[1].name = "Verificação (T1 e T2)"
fig.add_bar(y=[Y[1]], x=[med["sem_gx"]], name="Pipeline sem verificação", orientation="h", marker_color=CINZA_CL,
            text=[f"{br(med['sem_gx'], 1)} s"], textposition="outside", showlegend=False, hovertemplate="sem verificação: %{x:.1f} s<extra></extra>")
fig.add_annotation(y=Y[0], x=etp.sum(), text=f"  {br(med['com_gx'], 1)} s", showarrow=False, xanchor="left", font=dict(size=14))
fig.update_layout(barmode="stack", bargap=0.45)
fig.update_xaxes(title="Segundos por lote (mediana)", range=[0, med["com_gx"] * 1.15], showgrid=True, gridcolor="#F0F1F3")
fig.update_yaxes(autorange="reversed", title=None)
mostra(estilo(fig, 300), "fig-5-custo-tempo")
leitura(f"a diferença ({br(med['com_gx'] - med['sem_gx'], 0)} s por lote) está quase toda nas barras azuis, isto é, nas "
        "próprias verificações; a gravação dos alertas e das camadas leva poucos segundos.")

# ------------------------------------------------------------------ 6. metadados
secao(6, "Que metadados as verificações geram?", "Registros de observabilidade gravados no Unity Catalog",
      "A cada lote, as verificações gravam três tabelas: um <b>alerta</b> por regra violada em cada ponto, um "
      "<b>evento</b> por registro sinalizado (regra, atributo, ponto e instante) e a <b>quarentena</b>, com o registro completo para revisão.")
alertas["cenario_id"] = alertas.cenario_id.map(rotulo)
alertas["n_violacoes"] = alertas.n_violacoes.astype(int).map(br)
st.markdown("**Alertas (tabela `dq_alertas`) — primeiros lotes**")
tabela(alertas, ["Cenário", "Ponto", "Dimensão", "Registros sinalizados", "Instante (UTC)"], "fig-6a-metadados-alertas", [3.4, 1, 1.8, 2, 1.8])
st.markdown("**Eventos por registro (tabela `dq_eventos`) — lote 2, unicidade**")
eventos["atributo"] = eventos.atributo.map(lambda v: f"chave ({len(v.split(','))} atributos)" if "," in v and len(v.split(",")) > 2 else v.replace(",", ", "))
tabela(eventos, ["Registro", "Regra", "Dimensão", "Atributos", "Ponto", "Instante (UTC)", "Situação"], "fig-6b-metadados-eventos",
       [4.2, 1, 1.5, 2.4, 0.8, 1.5, 1.2])
st.markdown("**Quarentena (tabela `dq_quarentena`) — lote 14, data impossível**")
tabela(quar, ["Registro", "Regras violadas", "Data de notificação (texto)", "Município", "Gravado em (UTC)", "Situação"],
       "fig-6c-quarentena", [2.6, 2, 2.2, 1.3, 1.6, 1.8])

# ------------------------------------------------------------------ 7. data impossível
secao(7, "A preservação do texto original das datas tem efeito prático?", "Registros com data impossível na Silver",
      "Cenário de data impossível (lote 14): a data de notificação foi trocada por 20241332 (mês 13). A Silver guarda o texto "
      "original, a data convertida e uma marca de falha de conversão, o que permite ligar o nulo à regra de validade.")
vi = q(f"""select s.row_id, r.valor_antes, s.DT_NOTIFIC, coalesce(cast(s.data_notificacao as string), 'nulo') conv,
           cast(s.falha_conv_data_notificacao as string) falha, array_join(s.violacoes_t1, ', ') viol
           from {S}.silver_lote s join {S}.referencia_experimental r on r.run_id = s.run_id and r.row_id = s.row_id
           where s.run_id = '{EXP}-L14-VALIMP-BRZ-S43' order by s.row_id limit 6""")
vi["falha"] = vi.falha.map({"true": "sim", "false": "não"})
tabela(vi, ["Registro", "Data original", "Data recebida (texto preservado)", "Data convertida", "Falha de conversão", "Regra violada em T1"],
       "fig-7-data-impossivel", [2.4, 1.5, 2.6, 1.6, 1.7, 1.8])
vt = q(f"""select count(*) n, count_if(falha_conv_data_notificacao) f,
           count_if(falha_conv_data_notificacao and array_contains(violacoes_t1, 'R_VAL')) fv
           from {S}.silver_lote where run_id = '{EXP}-L14-VALIMP-BRZ-S43'""").iloc[0]
leitura(f"no lote 14, {br(int(vt.f))} registros tiveram falha de conversão e todos os {br(int(vt.fv))} foram sinalizados "
        "pela regra de validade, com o texto original preservado para consulta.")

st.caption(f"Fonte dos dados: {S} (Unity Catalog) e system.lakeflow · experimento 2 · {EXP}")
