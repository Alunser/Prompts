# Prompt para Devin — Mapear Configurações de Performance nas Factories Kafka

## Contexto

As configurações de performance do Kafka precisam ser mapeadas em 3 lugares:
1. `KafkaConfiguration.cs` — adicionar novos campos no model
2. `KafkaConsumerFactory.cs` — mapear novos campos para o `ConsumerConfig`
3. `appsettings.json` — adicionar valores nos ambientes

O producer já tem os campos mapeados corretamente na factory.
O consumer está sem os campos de performance.

---

## ⚠️ Regras obrigatórias

1. **NÃO alterar lógica existente nas factories**
2. **NÃO alterar campos já existentes**
3. **Apenas adicionar os novos campos descritos abaixo**
4. **Manter os testes existentes funcionando**

---

## Alteração 1 — `KafkaConfiguration.cs`

`FX9.ProcessaMovimentoContabil.Domain.Core/Configurations/KafkaConfiguration.cs`

**Adicionar campos na classe `ConsumerConfiguration`:**

```csharp
public class ConsumerConfiguration
{
    // ✅ Campos existentes — não alterar
    public string BrokerUrl { get; set; }
    public string ClientId { get; set; }
    public string Debug { get; set; }
    public string GroupId { get; set; }
    public List<TopicConfiguration> TopicConfigurations { get; set; }

    // ✅ Novos campos de performance
    public int FetchMinBytes { get; set; } = 1;
    public int FetchWaitMaxMs { get; set; } = 10;
    public int SessionTimeoutMs { get; set; } = 30000;
    public int HeartbeatIntervalMs { get; set; } = 3000;
    public int MaxPollIntervalMs { get; set; } = 300000;
}
```

---

## Alteração 2 — `KafkaConsumerFactory.cs`

`FX9.ProcessaMovimentoContabil.Infra.Messaging/Factories/KafkaConsumerFactory.cs`

**Adicionar os novos campos no `ConsumerConfig` existente:**

```csharp
_consumerConfig = new ConsumerConfig
{
    // ✅ Campos existentes — não alterar
    BootstrapServers = kafkaConfiguration.ConsumerConfiguration.BrokerUrl,
    ClientId = kafkaConfiguration.ConsumerConfiguration.ClientId,
    SecurityProtocol = SecurityProtocol.Ssl,
    SslCaLocation = kafkaConfiguration.MaasCertStringsConfiguration.CaCertLocation,
    SslKeystoreLocation = kafkaConfiguration.MaasCertStringsConfiguration.P12Location,
    SslKeystorePassword = kafkaConfiguration.MaasCertStringsConfiguration.P12Password,
    GroupId = kafkaConfiguration.ConsumerConfiguration.GroupId,
    Debug = kafkaConfiguration.ConsumerConfiguration.Debug,
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,

    // ✅ Novos campos de performance
    FetchMinBytes = kafkaConfiguration.ConsumerConfiguration.FetchMinBytes,
    FetchWaitMaxMs = kafkaConfiguration.ConsumerConfiguration.FetchWaitMaxMs,
    SessionTimeoutMs = kafkaConfiguration.ConsumerConfiguration.SessionTimeoutMs,
    HeartbeatIntervalMs = kafkaConfiguration.ConsumerConfiguration.HeartbeatIntervalMs,
    MaxPollIntervalMs = kafkaConfiguration.ConsumerConfiguration.MaxPollIntervalMs,
};
```

---

## Alteração 3 — `appsettings.json` de todos os ambientes

Adicionar os novos campos na seção `ConsumerConfiguration`:

```json
{
  "KafkaConfiguration": {
    "ConsumerConfiguration": {
      "BrokerUrl": "...",
      "ClientId": "...",
      "GroupId": "...",

      "FetchMinBytes": 1,
      "FetchWaitMaxMs": 10,
      "SessionTimeoutMs": 30000,
      "HeartbeatIntervalMs": 3000,
      "MaxPollIntervalMs": 300000
    }
  }
}
```

> ⚠️ Aplicar em todos os ambientes:
> - `appsettings.json`
> - `appsettings.Development.json`
> - `appsettings.Homologacao.json`
> - `appsettings.Production.json`

---

## Verificar producer — já está correto

O `KafkaProducerFactory` já mapeia corretamente os campos:
- `BatchNumMessages` ✅
- `LingerMs` ✅
- `Acks = Acks.All` ✅ — manter assim, dados críticos
- `SocketKeepaliveEnable` ✅
- `SocketTimeoutMs` ✅
- `ReconnectBackoffMs` ✅
- `ReconnectBackoffMaxMs` ✅

> ⚠️ **NÃO alterar o `Acks`** — manter `Acks.All` para garantir que nenhum
> retorno seja perdido em caso de falha do líder.

---

## O que cada campo faz

| Campo | Valor | Descrição |
|---|---|---|
| `FetchMinBytes` | `1` | Retorna assim que tiver 1 byte — sem espera |
| `FetchWaitMaxMs` | `10` | Aguarda no máximo 10ms por fetch |
| `SessionTimeoutMs` | `30000` | 30s para detectar consumer morto |
| `HeartbeatIntervalMs` | `3000` | Heartbeat a cada 3s — mantém consumer ativo |
| `MaxPollIntervalMs` | `300000` | 5 min máximo entre polls |

---

## Testes unitários a ajustar

### `KafkaConsumerFactory` — verificar se tem testes que validam o `ConsumerConfig`

Se existirem testes que verificam os campos do `ConsumerConfig`, adicionar
verificação dos novos campos:

```csharp
[Fact]
public void CreateConsumer_DeveConfigurarCamposDePerformance()
{
    // Assert
    _consumerConfig.FetchMinBytes.Should().Be(1);
    _consumerConfig.FetchWaitMaxMs.Should().Be(10);
    _consumerConfig.SessionTimeoutMs.Should().Be(30000);
    _consumerConfig.HeartbeatIntervalMs.Should().Be(3000);
    _consumerConfig.MaxPollIntervalMs.Should().Be(300000);
}
```

---

## Impacto esperado após as 3 alterações

| Métrica | Antes | Depois |
|---|---|---|
| Kafka publish | ~72ms | ~10-20ms |
| Kafka commit | ~64ms | ~10-20ms |
| Tempo total/msg | ~164ms | ~55-80ms |
| Throughput 7 tasks | ~42 msg/s | ~90-130 msg/s |
| 1M mensagens | ~6.6h | ~2-3h |

---

## Checklist final

- [ ] `ConsumerConfiguration` com 5 novos campos em `KafkaConfiguration.cs`
- [ ] `KafkaConsumerFactory` mapeando os 5 novos campos no `ConsumerConfig`
- [ ] `appsettings.json` atualizado em todos os ambientes
- [ ] `Acks.Leader` avaliado e aplicado se aprovado
- [ ] Testes da factory atualizados com os novos campos
- [ ] Projeto compila sem erros
- [ ] Todos os testes passando
- [ ] Nenhuma lógica existente alterada
