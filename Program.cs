// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  KafkaProducer - RecebimentoMovimentoContabil                           ║
// ║                                                                          ║
// ║  Dependências (adicionar ao .csproj):                                   ║
// ║    <PackageReference Include="Confluent.Kafka" Version="2.4.0" />       ║
// ║    <PackageReference Include="Confluent.SchemaRegistry" Version="2.4.0" />
// ║    <PackageReference Include="Confluent.SchemaRegistry.Serdes.Avro" Version="2.4.0" />
// ║    <PackageReference Include="Itau.KaasCertClient" Version="*" />       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Itau.KaasCertClient;
using System.Text.Json;

// ─── Configurações ────────────────────────────────────────────────────────────
const string BROKER_URL       = "kafka-events.dev.aws.cloud.ihf:31101";
const string CLIENT_ID        = "fx9_client_id_emt_ret_movimento_contabil_dev";
const string TOPIC            = "emprestimos-e-financiamentos-recebimentos-movimento-contabil";
const string SCHEMA_REGISTRY  = "https://schema-registry.dev.aws.cloud.ihf:8082";
const string CA_CERT          = "./certs/caroot.crt";
const string P12_LOCATION     = "./certs/FX90008.p12";
const string P12_PASSWORD     = "tj3hm@^^NrLG+SJp";
const string CLIENT_USER      = "FX90008";
const string CLIENT_PASSWORD  = "n0tNKg3SHWpaXsTdm2QAiZVrFH1j1WPeHVenqawdSQp8x7fr8FYQ==";
const string KAAS_ENVIRONMENT = "Development";
const string KAAS_APP_NAME    = "Worker Processar Movimento Contabil";
const string KAAS_COMMUNITY   = "CONSORCIO";
const string KAAS_SIGLA       = "FX9";

// ─── Banner ───────────────────────────────────────────────────────────────────
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine(@"
  ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗
  ██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗
  █████╔╝ ███████║█████╗  █████╔╝ ███████║
  ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║
  ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║
  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
  Producer Console - RecebimentoMovimentoContabil
");
Console.ResetColor();

Log($"Broker      : {BROKER_URL}");
Log($"Tópico      : {TOPIC}");
Log($"Schema Reg. : {SCHEMA_REGISTRY}");
Log($"ClientId    : {CLIENT_ID}");
Console.WriteLine();

// ─── Geração de Certificado via KaasCertClient ───────────────────────────────
Log("Gerando certificado via KAAS...");
try
{
    await KaasCertClientManager.NewConfigure(KAAS_ENVIRONMENT)
        .WithAppName(KAAS_APP_NAME)
        .WithCommunity(KAAS_COMMUNITY)
        .WithSigla(KAAS_SIGLA)
        .WithCaCertLocation(CA_CERT)
        .WithClientUserAndPassword(CLIENT_USER, CLIENT_PASSWORD)
        .WithP12LocationAndPassword(P12_LOCATION, P12_PASSWORD)
        .GoConfigure();
    LogSuccess("Certificado gerado com sucesso.");
}
catch (Exception ex)
{
    LogError($"Falha ao gerar certificado: {ex.Message}");
    return;
}
Console.WriteLine();

// ─── Schema Registry ──────────────────────────────────────────────────────────
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url                              = SCHEMA_REGISTRY,
    RequestTimeoutMs                 = 5000,
    MaxCachedSchemas                 = 10,
    EnableSslCertificateVerification = false,
};

// ─── Producer Config ──────────────────────────────────────────────────────────
var producerConfig = new ProducerConfig
{
    BootstrapServers                 = BROKER_URL,
    ClientId                         = CLIENT_ID,
    CompressionType                  = CompressionType.Gzip,
    BatchNumMessages                 = 100,
    LingerMs                         = 5,
    Acks                             = Acks.All,
    SecurityProtocol                 = SecurityProtocol.Ssl,
    SslCaLocation                    = CA_CERT,
    SslKeystoreLocation              = P12_LOCATION,
    SslKeystorePassword              = P12_PASSWORD,
    EnableSslCertificateVerification = false,
};

// ─── Schema ───────────────────────────────────────────────────────────────────
using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

// Busca o schema registrado no registry
Log("Buscando schema no registry...");
var registeredSchema = await schemaRegistry.GetLatestSchemaAsync(
    $"{TOPIC}-value");
var avroSchema = (RecordSchema)Schema.Parse(registeredSchema.SchemaString);
LogSuccess($"Schema obtido: versão {registeredSchema.Version}");
Console.WriteLine();

var avroSerializerConfig = new AvroSerializerConfig
{
    AutoRegisterSchemas = false,
    SubjectNameStrategy = SubjectNameStrategy.TopicRecord,
};

using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
    .SetErrorHandler((_, e) => LogError($"Erro no producer: {e.Reason}"))
    .Build();

LogSuccess("Producer conectado! Aguardando mensagens...\n");

// ─── Loop Principal ───────────────────────────────────────────────────────────
while (true)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("Opções:");
    Console.WriteLine("  [1] Enviar mensagem de exemplo");
    Console.WriteLine("  [2] Enviar mensagem customizada (JSON)");
    Console.WriteLine("  [3] Envio em lote (bulk)");
    Console.WriteLine("  [Q] Sair");
    Console.ResetColor();
    Console.Write("\nEscolha: ");

    var opcao = Console.ReadLine()?.Trim().ToUpper();

    switch (opcao)
    {
        case "1":
            await EnviarExemplo(producer, avroSchema);
            break;
        case "2":
            await EnviarCustomizado(producer, avroSchema);
            break;
        case "3":
            await EnviarLote(producer, avroSchema);
            break;
        case "Q":
            LogSuccess("Encerrando e fazendo flush...");
            producer.Flush(TimeSpan.FromSeconds(10));
            return;
        default:
            LogError("Opção inválida.");
            break;
    }

    Console.WriteLine();
}

// ─── Envio de exemplo ─────────────────────────────────────────────────────────
async Task EnviarExemplo(IProducer<string, GenericRecord> prod, RecordSchema schema, int seq = 1)
{
    var record = MensagemExemplo(schema, seq);
    var key    = record["data"] is GenericRecord d ? d["codigo_identificacao_movimentacao_financeira"]?.ToString() ?? $"KEY-{seq}" : $"KEY-{seq}";
    try
    {
        Log($"Publicando → key: {key}");
        var r = await prod.ProduceAsync(TOPIC, new Message<string, GenericRecord> { Key = key, Value = record });
        LogSuccess($"✔ Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (Exception ex) { LogError(ex.Message); }
}

// ─── Envio customizado via JSON ───────────────────────────────────────────────
async Task EnviarCustomizado(IProducer<string, GenericRecord> prod, RecordSchema schema)
{
    Console.WriteLine("Cole o JSON do campo 'data' e pressione Enter duas vezes:");
    var sb = new System.Text.StringBuilder();
    string? line;
    while (!string.IsNullOrWhiteSpace(line = Console.ReadLine()))
        sb.AppendLine(line);

    try
    {
        var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(sb.ToString(), opts)!;

        var dataSchema = (RecordSchema)schema.Fields.First(f => f.Name == "data").Schema;
        var dataRecord = new GenericRecord(dataSchema);
        foreach (var field in dataSchema.Fields)
        {
            if (dict.TryGetValue(field.Name, out var el))
                dataRecord.Add(field.Name, JsonElementParaObjeto(el));
            else
                dataRecord.Add(field.Name, null);
        }

        var record = new GenericRecord(schema);
        record.Add("data", dataRecord);

        var key = dataRecord["codigo_identificacao_movimentacao_financeira"]?.ToString() ?? "KEY-CUSTOM";
        var r   = await prod.ProduceAsync(TOPIC, new Message<string, GenericRecord> { Key = key, Value = record });
        LogSuccess($"✔ Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (JsonException jex) { LogError($"JSON inválido: {jex.Message}"); }
    catch (Exception ex)      { LogError(ex.Message); }
}

// ─── Envio em lote ────────────────────────────────────────────────────────────
async Task EnviarLote(IProducer<string, GenericRecord> prod, RecordSchema schema)
{
    Console.Write("Quantas mensagens? ");
    if (!int.TryParse(Console.ReadLine(), out var qtd) || qtd <= 0) { LogError("Quantidade inválida."); return; }

    Log($"Enviando {qtd} mensagens...");
    var tasks = Enumerable.Range(1, qtd).Select(i =>
    {
        var record = MensagemExemplo(schema, i);
        var key    = record["data"] is GenericRecord d ? d["codigo_identificacao_movimentacao_financeira"]?.ToString() ?? $"KEY-{i}" : $"KEY-{i}";
        return prod.ProduceAsync(TOPIC, new Message<string, GenericRecord> { Key = key, Value = record });
    }).ToList();

    try
    {
        var results = await Task.WhenAll(tasks);
        LogSuccess($"✔ {results.Length} mensagens publicadas.");
        foreach (var r in results)
            Console.WriteLine($"   → Key: {r.Key} | Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (Exception ex) { LogError(ex.Message); }
}

// ─── Monta GenericRecord de exemplo ──────────────────────────────────────────
GenericRecord MensagemExemplo(RecordSchema schema, int seq = 1)
{
    var dataSchema = (RecordSchema)schema.Fields.First(f => f.Name == "data").Schema;
    var dataRecord = new GenericRecord(dataSchema);

    dataRecord.Add("codigo_identificacao_movimentacao_financeira", $"MOVFIN-{DateTime.Now:yyyyMMddHHmmss}-{seq:D4}");
    dataRecord.Add("codigo_pessoa_corporativo",                    "12345678901");
    dataRecord.Add("codigo_tipo_pessoa_titular_recebivel",         "F");
    dataRecord.Add("numero_centro_custo_debito",                   "CC001");
    dataRecord.Add("numero_centro_custo_credito",                  "CC002");
    dataRecord.Add("codigo_identificador_referencia_movimento",    $"REF-{seq:D6}");
    dataRecord.Add("codigo_produto_operacional",                   100);
    dataRecord.Add("identificador_evento_negocio",                 DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + seq);
    dataRecord.Add("codigo_empresa",                               341);
    dataRecord.Add("identificador_grupo",                          1001.0);
    dataRecord.Add("numero_identificador_cota_cliente",            42.0);
    dataRecord.Add("numero_grupo_bem_produto_consorcio",           "GRP-0042");
    dataRecord.Add("numero_cota_consorcio",                        42);
    dataRecord.Add("numero_sequencial_versao",                     seq);
    dataRecord.Add("numero_contrato",                              9876543210L + seq);
    dataRecord.Add("data_hora_venda",                              DateTime.Now.AddDays(-30).ToString("yyyy-MM-ddTHH:mm:ss"));
    dataRecord.Add("data_contemplacao_consorcio",                  null);
    dataRecord.Add("data_entrega_bem_consorcio",                   null);
    dataRecord.Add("codigo_situacao_cobranca",                     "A");
    dataRecord.Add("data_cancelamento_cota",                       null);
    dataRecord.Add("codigo_situacao_grupo",                        "A");
    dataRecord.Add("codigo_tipo_empresa_origem",                   "341");
    dataRecord.Add("codigo_empresa_origem",                        "0001");
    dataRecord.Add("codigo_dependencia_origem",                    "0001");
    dataRecord.Add("codigo_tipo_empresa_destino",                  "341");
    dataRecord.Add("codigo_empresa_destino",                       "0002");
    dataRecord.Add("codigo_dependencia_destino",                   "0002");
    dataRecord.Add("indicador_status_estorno",                     null);
    dataRecord.Add("sigla_sistema_evento",                         "CONS");
    dataRecord.Add("numero_parcela_contrato",                      1);
    dataRecord.Add("numero_prazo_cota",                            60);
    dataRecord.Add("data_vencimento_parcela_contrato",             DateTime.Now.AddMonths(1).ToString("yyyy-MM-dd"));
    dataRecord.Add("data_contabil_transacao",                      DateTime.Now.ToString("yyyy-MM-dd"));
    dataRecord.Add("indicador_tributacao_imposto",                 null);
    dataRecord.Add("valor_fundo_comum",                            1500.0);
    dataRecord.Add("valor_fundo_reserva",                          150.0);
    dataRecord.Add("valor_taxa_administracao_paga",                75.0);
    dataRecord.Add("valor_seguro_pagar",                           30.0);
    dataRecord.Add("valor_multa_juro_pagar",                       null);
    dataRecord.Add("valor_multa_juro_administradora",              null);
    dataRecord.Add("valor_outro",                                  null);
    dataRecord.Add("valor_total_grupo",                            5000.0);
    dataRecord.Add("valor_total_demais_parcela",                   null);
    dataRecord.Add("valor_total_lancamento",                       1755.0);
    dataRecord.Add("valor_total_tributacao",                       null);
    dataRecord.Add("indicador_tarifa_movimento",                   null);
    dataRecord.Add("data_operacao_origem",                         DateTime.Now.ToString("yyyy-MM-dd"));
    dataRecord.Add("codigo_tipo_motivo_estorno",                   null);
    dataRecord.Add("codigo_unico_transacao_origem",                null);
    dataRecord.Add("codigo_sistema_integrador",                    "FX9");
    dataRecord.Add("indicador_grupo_antes_lei",                    null);

    var record = new GenericRecord(schema);
    record.Add("data", dataRecord);
    return record;
}

// ─── Converte JsonElement para tipo nativo ────────────────────────────────────
object? JsonElementParaObjeto(JsonElement el) => el.ValueKind switch
{
    JsonValueKind.String  => el.GetString(),
    JsonValueKind.Number  => el.TryGetInt64(out var l) ? l : el.TryGetDouble(out var d) ? d : (object?)el.GetInt32(),
    JsonValueKind.True    => true,
    JsonValueKind.False   => false,
    JsonValueKind.Null    => null,
    _                     => el.ToString()
};

// ─── Helpers de log ───────────────────────────────────────────────────────────
void Log(string msg)        { Console.ForegroundColor = ConsoleColor.Gray;  Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");    Console.ResetColor(); }
void LogSuccess(string msg) { Console.ForegroundColor = ConsoleColor.Green; Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");    Console.ResetColor(); }
void LogError(string msg)   { Console.ForegroundColor = ConsoleColor.Red;   Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] ✖ {msg}"); Console.ResetColor(); }
