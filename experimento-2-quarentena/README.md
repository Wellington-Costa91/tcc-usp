# Experimento 2 · quarentena (`EXP-2026-11-09`)

Segunda execução, **isolada** da primeira (`EXP-2026-10-04`): schema próprio (`tcc_dq_exp2`), job próprio e app
próprio. Mesmo desenho, mesmas regras (R-1.0), mesmas sementes e mesma ordem dos lotes. A única diferença está na
construção da Gold:

- todo registro sinalizado em T1 ou T2 é gravado em **`dq_quarentena`** (registro completo + regras violadas +
  situação `pendente_revisao`) **antes** da construção da Gold;
- a Gold é construída **só com os registros não sinalizados**.

## Resultado

| Item | Valor |
|---|---|
| Lotes | 50 de 50 concluídos (job 1h55) |
| Detecção | idêntica ao experimento 1: VP = defeitos injetados, FN = 0, FP = 0 em todos os cenários |
| Sinalizações secundárias | 356, todas detectadas (iguais ao experimento 1) |
| Registros em quarentena | 1.045.990 |
| Registros sinalizados na Gold | **0** |
| Alerta e quarentena gravados antes da Gold | 45 de 45 lotes com defeito |
| Conferências da quarentena (`resultado_quarentena`) | 50 de 50 lotes em todas as cinco |
| Duração mediana do lote | 81,6 s com verificação × 10,1 s sem |

As cinco conferências por lote:
1. a quarentena contém exatamente os registros sinalizados;
2. a Gold não contém nenhum registro sinalizado;
3. quarentena + Gold = todos os registros da Silver;
4. a quarentena coincide com a rotina de referência independente;
5. a Gold é idêntica à construída a partir da base de referência sem os registros em quarentena.

## Estrutura

```
notebooks/         00 a 04 (diferenças em relação ao experimento 1: 00_comum, 03_execucao_lotes e 04_avaliacao)
job/               definição do job (troque <PASTA_NO_WORKSPACE>)
resultados/        tabelas exportadas pela tarefa de avaliação (inclui resultado_quarentena e amostra_dq_quarentena)
observabilidade/
  app/             Databricks App (schema configurado em app.yaml)
  figuras/         figuras exportadas do app
  dados-graficos/  ★ dados brutos dos gráficos: um CSV por figura, com exatamente os dados plotados (ver INDICE.csv)
```

Os dados de origem são os mesmos do experimento 1 (`../dados/`, conferidos por `../SHA256SUMS`). Para reproduzir, siga o README de `../experimento-1-sinalizacao/` trocando o
schema por `tcc_dq_exp2`. Para regerar `dados-graficos/`:

```
python3 ../ferramentas/gerar-dados-graficos.py well_costa_classic_stable_catalog.tcc_dq_exp2 EXP-2026-11-09 observabilidade/dados-graficos --quarentena
```
