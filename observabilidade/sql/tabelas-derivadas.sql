-- Tabelas derivadas usadas pelo app de observabilidade (criadas em 2026-11-09 a partir da execução EXP-2026-10-04).
-- Não alteram as tabelas do experimento; troque o catálogo se necessário.

-- Grupos da Gold (município × mês) que receberam registros sinalizados, por lote, via gold_linhagem.
CREATE OR REPLACE TABLE well_costa_classic_stable_catalog.tcc_dq_exp.obs_gold_afetada AS
WITH ev AS (
  SELECT DISTINCT run_id, row_id FROM well_costa_classic_stable_catalog.tcc_dq_exp.dq_eventos WHERE status = 'violacao'
),
l AS (
  SELECT l.run_id, l.gold_key, l.row_id, ev.row_id IS NOT NULL AS sinalizado
  FROM well_costa_classic_stable_catalog.tcc_dq_exp.gold_linhagem l
  LEFT JOIN ev ON l.run_id = ev.run_id AND l.row_id = ev.row_id
)
SELECT m.ordem, m.cenario_id, l.run_id,
       count(DISTINCT gold_key)                                   AS grupos_total,
       count(DISTINCT CASE WHEN sinalizado THEN gold_key END)     AS grupos_afetados,
       count(*)                                                   AS registros_gold,
       sum(CAST(sinalizado AS INT))                               AS registros_sinalizados
FROM l JOIN well_costa_classic_stable_catalog.tcc_dq_exp.manifesto_execucao m ON m.run_id = l.run_id
GROUP BY ALL;

-- Linha do tempo das 4 tarefas do job da execução de referência (system.lakeflow).
CREATE OR REPLACE TABLE well_costa_classic_stable_catalog.tcc_dq_exp.obs_job_tarefas AS
SELECT task_key, min(period_start_time) AS inicio, max(period_end_time) AS fim,
       max_by(result_state, period_end_time) AS resultado
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '20157742948129' AND job_run_id = '821477330355732'
GROUP BY task_key;
