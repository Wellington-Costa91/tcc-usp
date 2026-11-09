# Observabilidade da execução (app e figuras)

Material acrescentado em 2026-11-09, depois da versão citada no TCC (commit `9b6ccbc`). Não altera o
experimento: lê as tabelas geradas pela execução `EXP-2026-10-04` e produz as figuras de observabilidade.

```
app/        Databricks App (Streamlit + Plotly) que monta as figuras a partir do schema tcc_dq_exp
figuras/    figuras exportadas do app (PNG, alta resolução)
dados/      tabelas usadas pelo app que não estão em ../resultados/
sql/        criação das tabelas derivadas obs_gold_afetada e obs_job_tarefas
```

## Figuras

| Arquivo | O que mostra | Tabelas de origem |
|---|---|---|
| `fig-1-execucao-job.png` | as 4 tarefas do job e sua duração | `obs_job_tarefas` (system.lakeflow) |
| `fig-2-matriz-deteccao.png` | fração dos registros com defeito sinalizada por regra e ponto, por cenário | `dq_resumo_regras` |
| `fig-3-linha-do-tempo-lotes.png` | etapas, alertas e início da Gold em cada lote (relógio do controlador) | `manifesto_execucao` (marcas) |
| `fig-4-gold-grupos-afetados.png` | grupos da Gold que receberam registros sinalizados | `obs_gold_afetada` |
| `fig-5-custo-tempo.png` | tempo mediano por etapa, com × sem verificação | `tempo_execucao`, `manifesto_execucao` |
| `fig-6a/6b-metadados-*.png` | amostras de `dq_alertas` e `dq_eventos` | `dq_alertas`, `dq_eventos` |
| `fig-7-data-impossivel.png` | datas impossíveis preservadas na Silver (lote 14) | `silver_lote`, `referencia_experimental` |

`dados/tempo_execucao.csv` e `dados/resultado_tempo.csv` vêm do módulo opcional de medição de tempo
(`modulo_tempo` em `03_execucao_lotes.py`). Na versão citada, essas medições não fazem parte da análise do TCC.

## Como publicar o app

1. Crie as tabelas de `sql/tabelas-derivadas.sql`.
2. Crie um Databricks App com o recurso **SQL warehouse** de nome `sql-warehouse` (permissão *Can use*).
3. Conceda à identidade do app `USE CATALOG`, `USE SCHEMA` e `SELECT` no schema `tcc_dq_exp`.
4. Publique a pasta `app/`: `databricks apps deploy <nome> --source-code-path /Workspace/<pasta>`.

Cada gráfico tem um botão de câmera que baixa o PNG em alta resolução.
