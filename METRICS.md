# Métricas do Ducklake Kafka Connect

## Visão Geral

O conector Ducklake expõe métricas detalhadas sobre o desempenho das operações JDBC e de schema através do sistema de métricas padrão do Kafka Connect. Essas métricas são automaticamente expostas via JMX e podem ser acessadas através de ferramentas de monitoramento padrão.

**✨ Novo:** Cada query JDBC agora é rastreada individualmente por tipo de operação (`upsertWithMergeInto`, `simpleInsert`, `createTable`, `evolveSchema`) permitindo análise granular de performance.

## Métricas Disponíveis

### 1. Métricas de Query JDBC (Agregadas)

**Grupo:** `ducklake-sink-task-metrics`

- **`jdbc-query-time-avg`** (ms)
  - Tempo médio de execução de TODAS as queries JDBC
  - Útil para visão geral de performance

- **`jdbc-query-time-max`** (ms)
  - Tempo máximo de execução entre todas as queries
  - Identifica queries lentas

- **`jdbc-query-count`** (total)
  - Número total de queries JDBC executadas
  - Útil para calcular throughput geral

- **`jdbc-query-rate`** (queries/segundo)
  - Taxa de execução de queries por segundo
  - Monitora a carga no banco de dados

### 2. Métricas por Tipo de Operação 🆕

Cada operação tem métricas específicas com a tag `operation`:

#### MERGE INTO (Upsert)
- **`operation-time-avg{operation="upsertWithMergeInto"}`** (ms)
  - Tempo médio de execução de MERGE INTO queries
  - Útil para monitorar performance de upserts com primary keys

- **`operation-time-max{operation="upsertWithMergeInto"}`** (ms)
  - Tempo máximo de MERGE INTO
  - Identifica upserts problemáticos

- **`operation-count{operation="upsertWithMergeInto"}`** (total)
  - Número total de MERGE INTO executados

- **`operation-rate{operation="upsertWithMergeInto"}`** (ops/segundo)
  - Taxa de upserts por segundo

#### INSERT Simples
- **`operation-time-avg{operation="simpleInsert"}`** (ms)
  - Tempo médio de execução de INSERT queries
  - Útil para monitorar performance de inserts sem upsert

- **`operation-time-max{operation="simpleInsert"}`** (ms)
  - Tempo máximo de INSERT

- **`operation-count{operation="simpleInsert"}`** (total)
  - Número total de INSERTs executados

- **`operation-rate{operation="simpleInsert"}`** (ops/segundo)
  - Taxa de inserts por segundo

#### CREATE TABLE
- **`operation-time-avg{operation="createTable"}`** (ms)
  - Tempo médio para criar tabelas
  - Útil para monitorar auto-create performance

- **`operation-count{operation="createTable"}`** (total)
  - Número de tabelas criadas automaticamente

#### EVOLVE SCHEMA
- **`operation-time-avg{operation="evolveSchema"}`** (ms)
  - Tempo médio para evoluir schema (ADD COLUMN)
  - Útil para monitorar schema evolution

- **`operation-count{operation="evolveSchema"}`** (total)
  - Número de schema evolutions executadas

### 3. Métricas de Operações de Schema (Agregadas)

- **`schema-operation-time-avg`** (ms)
  - Tempo médio de TODAS as operações DDL
  - Monitora performance geral de schema

- **`schema-operation-time-max`** (ms)
  - Tempo máximo de uma operação de schema

- **`schema-operation-count`** (total)
  - Número total de operações de schema

### 4. Métricas de Processamento de Records

- **`records-processed-total`** (total)
  - Número total de records processados
  - Monitora volume de dados

- **`records-processed-rate`** (records/segundo)
  - Taxa de processamento de records
  - Mede throughput do conector

### 5. Métricas de Batch

- **`batch-size-avg`** (records)
  - Tamanho médio dos batches processados
  - Otimização de configuração

- **`batch-size-max`** (records)
  - Tamanho máximo de batch processado
  - Identifica picos de carga

## Tags (Labels) das Métricas

Todas as métricas incluem as seguintes tags para filtragem e agregação:

- **`connector`**: Nome da instância do conector (ex: `ducklake-sink`)
- **`task`**: ID da task do conector (ex: `0`, `1`, `2`)
- **`operation`** (métricas específicas): Tipo de operação (`upsertWithMergeInto`, `simpleInsert`, `createTable`, `evolveSchema`)

## Exemplos de Queries Prometheus

### Comparar Performance de INSERT vs MERGE
```promql
# Tempo médio de MERGE
operation_time_avg{operation="upsertWithMergeInto", connector="ducklake-sink"}

# Tempo médio de INSERT
operation_time_avg{operation="simpleInsert", connector="ducklake-sink"}

# Diferença percentual
(operation_time_avg{operation="upsertWithMergeInto"} - operation_time_avg{operation="simpleInsert"}) 
/ operation_time_avg{operation="simpleInsert"} * 100
```

### Taxa de Upserts vs Inserts
```promql
# Upserts por segundo
operation_rate{operation="upsertWithMergeInto", connector="ducklake-sink"}

# Inserts por segundo
operation_rate{operation="simpleInsert", connector="ducklake-sink"}

# Total de operações por segundo
sum(operation_rate{connector="ducklake-sink"})
```

### Queries Mais Lentas por Tipo
```promql
# Tempo máximo por tipo de operação
operation_time_max{connector="ducklake-sink"}

# Top operações por tempo médio
topk(5, operation_time_avg{connector="ducklake-sink"})
```

### Monitorar Schema Evolution
```promql
# Quantas vezes schema foi alterado nas últimas 24h
increase(operation_count{operation="evolveSchema"}[24h])

# Taxa de schema changes por hora
rate(operation_count{operation="evolveSchema"}[1h]) * 3600
```

### Alertas Recomendados por Operação

#### MERGE Lento
```yaml
- alert: DucklakeMergeSlowQuery
  expr: operation_time_avg{operation="upsertWithMergeInto"} > 1000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "MERGE queries estão lentas"
    description: "Tempo médio de MERGE: {{ $value }}ms no connector {{ $labels.connector }}"
```

#### INSERT Lento
```yaml
- alert: DucklakeInsertSlowQuery
  expr: operation_time_avg{operation="simpleInsert"} > 500
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "INSERT queries estão lentas"
    description: "Tempo médio de INSERT: {{ $value }}ms no connector {{ $labels.connector }}"
```

#### Schema Changes Frequentes
```yaml
- alert: DucklakeFrequentSchemaChanges
  expr: rate(operation_count{operation="evolveSchema"}[5m]) > 0.1
  for: 5m
  labels:
    severity: info
  annotations:
    summary: "Schema está evoluindo frequentemente"
    description: "Taxa de schema changes: {{ $value }}/s no connector {{ $labels.connector }}"
```

#### Muitas Criações de Tabela
```yaml
- alert: DucklakeFrequentTableCreation
  expr: rate(operation_count{operation="createTable"}[10m]) > 0.05
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Muitas tabelas sendo criadas"
    description: "{{ $value }} tabelas criadas/s - verificar se topics estão corretos"
```

## Dashboard Grafana

### Painel: Comparação de Performance de Operações

```promql
# Query 1: Tempo médio por tipo de operação
operation_time_avg{connector="ducklake-sink"}

# Query 2: Taxa de operações por tipo
operation_rate{connector="ducklake-sink"}

# Query 3: Contagem de operações por tipo (24h)
increase(operation_count{connector="ducklake-sink"}[24h])
```

### Painel: Performance de Upsert (MERGE)

```promql
# Tempo médio
operation_time_avg{operation="upsertWithMergeInto"}

# Tempo P95
histogram_quantile(0.95, operation_time_avg{operation="upsertWithMergeInto"})

# Taxa
operation_rate{operation="upsertWithMergeInto"}
```

### Painel: Schema Operations Timeline

```promql
# Schema changes ao longo do tempo
increase(operation_count{operation="evolveSchema"}[1h])

# Tabelas criadas ao longo do tempo
increase(operation_count{operation="createTable"}[1h])
```

## Como Acessar as Métricas

### 1. Via JMX

```bash
jconsole <pid-do-kafka-connect>
```

Navegue para:
- `org.apache.kafka.common.metrics`
- Procure por métricas com `operation` tag

### 2. Via Prometheus

Com JMX Exporter configurado:
```bash
curl http://localhost:7071/metrics | grep operation_time_avg
```

Exemplo de output:
```
operation_time_avg{connector="ducklake-sink",operation="upsertWithMergeInto",task="0"} 45.2
operation_time_avg{connector="ducklake-sink",operation="simpleInsert",task="0"} 12.8
operation_time_avg{connector="ducklake-sink",operation="createTable",task="0"} 234.5
operation_time_avg{connector="ducklake-sink",operation="evolveSchema",task="0"} 156.3
```

### 3. Via Grafana

Importe o dashboard em `docs/grafana-dashboard.json` que já inclui painéis para métricas por operação.

## Interpretação das Métricas

### Performance Esperada

| Operação | Tempo Médio Esperado | Observações |
|----------|---------------------|-------------|
| `simpleInsert` | 10-50ms | Mais rápido, sem merge |
| `upsertWithMergeInto` | 30-100ms | Mais lento por verificar duplicatas |
| `createTable` | 100-500ms | Executado raramente |
| `evolveSchema` | 50-200ms | Depende do número de colunas |

### Quando Investigar

- **MERGE > 500ms**: Possível problema de índice ou volume alto
- **INSERT > 100ms**: Possível contenção ou I/O lento
- **CREATE TABLE frequente**: Pode indicar erro na configuração de topics
- **EVOLVE SCHEMA frequente**: Schema instável ou dados inconsistentes

## Troubleshooting

### Métricas por operação não aparecem

1. Verifique se está usando timers específicos:
```java
try (var timer = metrics.startJdbcQueryTimer("upsertWithMergeInto")) {
    // query
}
```

2. Verifique logs para mensagens de warning sobre operações desconhecidas

### Valores inesperados

- **MERGE mais rápido que INSERT**: Possível quando não há duplicatas
- **CREATE TABLE constante**: Verifique se `auto.create.table` está habilitado incorretamente
- **EVOLVE SCHEMA frequente**: Schema dos dados está mudando constantemente

## Referências

- [Kafka Metrics](https://kafka.apache.org/documentation/#monitoring)
- [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter)
- [Grafana Dashboards](https://grafana.com/grafana/dashboards/)

