# Observabilidade da qualidade de dados em pipelines de processamento em lote com Great Expectations

Material de reprodução do Trabalho de Conclusão de Curso da Especialização em Engenharia de Dados e Big Data
(PECE, Escola Politécnica da USP, 2026).

- **Autor:** Wellington Antonio de Oliveira Costa
- **Orientador:** Wesley Lourenço Barbosa

## Dois experimentos

| | [Experimento 1 · sinalização](experimento-1-sinalizacao/) | [Experimento 2 · quarentena](experimento-2-quarentena/) |
|---|---|---|
| Execução | `EXP-2026-10-04` | `EXP-2026-11-09` |
| Schema | `tcc_dq_exp` | `tcc_dq_exp2` (isolado) |
| Registros sinalizados | seguem marcados até a Gold | vão para `dq_quarentena`; a Gold recebe só os demais |
| Sinalizados na Gold | 1.045.990 (rastreáveis pela linhagem) | **0** |
| Detecção (VP / FN / FP) | igual nos dois: todos os defeitos detectados, FN = 0, FP = 0 | |
| Alerta antes da Gold | 45 de 45 lotes com defeito | 45 de 45 (alerta e quarentena) |

Os dois usam os mesmos dados de origem, regras (R-1.0), sementes e ordem dos lotes. O experimento 1 é a versão
citada no TCC (commit `9b6ccbc`).

## Estrutura

```
dados/                       arquivos de origem (DATASUS) e domínio municipal (IBGE), comuns aos dois experimentos
SHA256SUMS                   hashes dos arquivos de dados
experimento-1-sinalizacao/   notebooks, job, resultados, evidências, diagrama e observabilidade (app, figuras, dados-graficos)
experimento-2-quarentena/    notebooks, job, resultados e observabilidade (app, figuras, dados-graficos)
ferramentas/                 gerar-dados-graficos.py (gera um CSV por figura a partir do schema)
```

## Dados dos gráficos

Para refazer os gráficos em qualquer ferramenta, sem depender do app, use as pastas
`experimento-*/observabilidade/dados-graficos/`: um CSV por figura, com o mesmo nome da figura, e um `INDICE.csv`
que descreve cada arquivo.
