// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  KafkaProducer - RecebimentoMovimentoContabil                           ║
// ║                                                                          ║
// ║  Dependências (adicionar ao .csproj):                                   ║
// ║    <PackageReference Include="Confluent.Kafka" Version="2.4.0" />       ║
// ║    <PackageReference Include="Confluent.SchemaRegistry" Version="2.4.0" />
// ║    <PackageReference Include="Confluent.SchemaRegistry.Serdes.Avro" Version="2.4.0" />
// ║    <PackageReference Include="Itau.KaasCertClient" Version="*" />       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Itau.KaasCertClient;
using System.Text.Json;
using System.Text.Json.Serialization;

// ─── Configurações ────────────────────────────────────────────────────────────
const string BROKER_URL        = "kafka-events.dev.aws.cloud.ihf:31101";
const string CLIENT_ID         = "fx9_client_id_emt_ret_movimento_contabil_dev";
const string TOPIC             = "emprestimos-e-financiamentos-recebimentos-movimento-contabil";
const string SCHEMA_REGISTRY   = "https://schema-registry.dev.aws.cloud.ihf:8082";
const string CA_CERT           = "./certs/caroot.crt";
const string P12_LOCATION      = "./certs/FX90008.p12";
const string P12_PASSWORD      = "tj3hm@^^NrLG+SJp";
const string CLIENT_USER       = "FX90008";
const string CLIENT_PASSWORD   = "n0tNKg3SHWpaXsTdm2QAiZVrFH1j1WPeHVenqawdSQp8x7fr8FYQ==";
const string KAAS_ENVIRONMENT  = "Development";
const string KAAS_APP_NAME     = "Worker Processar Movimento Contabil";
const string KAAS_COMMUNITY    = "CONSORCIO";
const string KAAS_SIGLA        = "FX9";

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
Log($"Cert P12    : {P12_LOCATION}");
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

// ─── Loop Principal ───────────────────────────────────────────────────────────
using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

var avroSerializerConfig = new AvroSerializerConfig
{
    AutoRegisterSchemas = false,
    UseLatestVersion    = true,
    SubjectNameStrategy = SubjectNameStrategy.TopicRecord,
};

using var producer = new ProducerBuilder<string, RecebimentoMovimentoContabil>(producerConfig)
    .SetValueSerializer(new AvroSerializer<RecebimentoMovimentoContabil>(schemaRegistry, avroSerializerConfig))
    .SetErrorHandler((_, e) => LogError($"Erro no producer: {e.Reason}"))
    .Build();

LogSuccess("Producer conectado! Aguardando mensagens...\n");

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
            await EnviarExemplo(producer);
            break;
        case "2":
            await EnviarCustomizado(producer);
            break;
        case "3":
            await EnviarLote(producer);
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
async Task EnviarExemplo(IProducer<string, RecebimentoMovimentoContabil> prod, int seq = 1)
{
    var msg = MensagemExemplo(seq);
    var key = msg.data.codigo_identificacao_movimentacao_financeira;
    try
    {
        Log($"Publicando → key: {key}");
        var r = await prod.ProduceAsync(TOPIC, new Message<string, RecebimentoMovimentoContabil> { Key = key, Value = msg });
        LogSuccess($"✔ Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (Exception ex) { LogError(ex.Message); }
}

// ─── Envio customizado via JSON ───────────────────────────────────────────────
async Task EnviarCustomizado(IProducer<string, RecebimentoMovimentoContabil> prod)
{
    Console.WriteLine("Cole o JSON (campo 'data') e pressione Enter duas vezes:");
    var sb = new System.Text.StringBuilder();
    string? line;
    while (!string.IsNullOrWhiteSpace(line = Console.ReadLine()))
        sb.AppendLine(line);

    try
    {
        var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var data = JsonSerializer.Deserialize<RecebimentoMovimentoContabilData>(sb.ToString(), opts)!;
        var msg  = new RecebimentoMovimentoContabil { data = data };
        var key  = data.codigo_identificacao_movimentacao_financeira;

        var r = await prod.ProduceAsync(TOPIC, new Message<string, RecebimentoMovimentoContabil> { Key = key, Value = msg });
        LogSuccess($"✔ Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (JsonException jex) { LogError($"JSON inválido: {jex.Message}"); }
    catch (Exception ex)      { LogError(ex.Message); }
}

// ─── Envio em lote ────────────────────────────────────────────────────────────
async Task EnviarLote(IProducer<string, RecebimentoMovimentoContabil> prod)
{
    Console.Write("Quantas mensagens? ");
    if (!int.TryParse(Console.ReadLine(), out var qtd) || qtd <= 0) { LogError("Quantidade inválida."); return; }

    Log($"Enviando {qtd} mensagens...");
    var tasks = Enumerable.Range(1, qtd).Select(i =>
    {
        var msg = MensagemExemplo(i);
        return prod.ProduceAsync(TOPIC, new Message<string, RecebimentoMovimentoContabil>
            { Key = msg.data.codigo_identificacao_movimentacao_financeira, Value = msg });
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

// ─── Mensagem de exemplo ──────────────────────────────────────────────────────
RecebimentoMovimentoContabil MensagemExemplo(int seq = 1) => new()
{
    data = new RecebimentoMovimentoContabilData
    {
        codigo_identificacao_movimentacao_financeira = $"MOVFIN-{DateTime.Now:yyyyMMddHHmmss}-{seq:D4}",
        codigo_pessoa_corporativo                   = "12345678901",
        codigo_tipo_pessoa_titular_recebivel        = "F",
        numero_centro_custo_debito                  = "CC001",
        numero_centro_custo_credito                 = "CC002",
        codigo_identificador_referencia_movimento   = $"REF-{seq:D6}",
        codigo_produto_operacional                  = 100,
        identificador_evento_negocio                = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + seq,
        codigo_empresa                              = 341,
        identificador_grupo                         = 1001.0,
        numero_identificador_cota_cliente           = 42.0,
        numero_grupo_bem_produto_consorcio          = "GRP-0042",
        numero_cota_consorcio                       = 42,
        numero_sequencial_versao                    = seq,
        numero_contrato                             = 9876543210L + seq,
        data_hora_venda                             = DateTime.Now.AddDays(-30).ToString("yyyy-MM-ddTHH:mm:ss"),
        codigo_situacao_cobranca                    = "A",
        codigo_situacao_grupo                       = "A",
        codigo_tipo_empresa_origem                  = "341",
        codigo_empresa_origem                       = "0001",
        codigo_dependencia_origem                   = "0001",
        codigo_tipo_empresa_destino                 = "341",
        codigo_empresa_destino                      = "0002",
        codigo_dependencia_destino                  = "0002",
        sigla_sistema_evento                        = "CONS",
        numero_parcela_contrato                     = 1,
        numero_prazo_cota                           = 60,
        data_vencimento_parcela_contrato            = DateTime.Now.AddMonths(1).ToString("yyyy-MM-dd"),
        data_contabil_transacao                     = DateTime.Now.ToString("yyyy-MM-dd"),
        valor_fundo_comum                           = 1500.00,
        valor_fundo_reserva                         = 150.00,
        valor_taxa_administracao_paga               = 75.00,
        valor_seguro_pagar                          = 30.00,
        valor_total_grupo                           = 5000.00,
        valor_total_lancamento                      = 1755.00,
        data_operacao_origem                        = DateTime.Now.ToString("yyyy-MM-dd"),
        codigo_sistema_integrador                   = "FX9",
    }
};

// ─── Helpers de log ───────────────────────────────────────────────────────────
void Log(string msg)        { Console.ForegroundColor = ConsoleColor.Gray;  Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");    Console.ResetColor(); }
void LogSuccess(string msg) { Console.ForegroundColor = ConsoleColor.Green; Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");    Console.ResetColor(); }
void LogError(string msg)   { Console.ForegroundColor = ConsoleColor.Red;   Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] ✖ {msg}"); Console.ResetColor(); }

// ─── Models ───────────────────────────────────────────────────────────────────
public class RecebimentoMovimentoContabil
{
    public RecebimentoMovimentoContabilData data { get; set; } = new();
}

public class RecebimentoMovimentoContabilData
{
    public string  codigo_identificacao_movimentacao_financeira { get; set; } = string.Empty;
    public string  codigo_pessoa_corporativo                    { get; set; } = string.Empty;
    public string  codigo_tipo_pessoa_titular_recebivel         { get; set; } = string.Empty;
    public string  numero_centro_custo_debito                   { get; set; } = string.Empty;
    public string  numero_centro_custo_credito                  { get; set; } = string.Empty;
    public string  codigo_identificador_referencia_movimento    { get; set; } = string.Empty;
    public int     codigo_produto_operacional                   { get; set; }
    public long    identificador_evento_negocio                 { get; set; }
    public int     codigo_empresa                               { get; set; }
    public double? identificador_grupo                          { get; set; }
    public double? numero_identificador_cota_cliente            { get; set; }
    public string  numero_grupo_bem_produto_consorcio           { get; set; } = string.Empty;
    public int     numero_cota_consorcio                        { get; set; }
    public int     numero_sequencial_versao                     { get; set; }
    public long    numero_contrato                              { get; set; }
    public string? data_hora_venda                              { get; set; }
    public string? data_contemplacao_consorcio                  { get; set; }
    public string? data_entrega_bem_consorcio                   { get; set; }
    public string? codigo_situacao_cobranca                     { get; set; }
    public string? data_cancelamento_cota                       { get; set; }
    public string? codigo_situacao_grupo                        { get; set; }
    public string  codigo_tipo_empresa_origem                   { get; set; } = string.Empty;
    public string  codigo_empresa_origem                        { get; set; } = string.Empty;
    public string  codigo_dependencia_origem                    { get; set; } = string.Empty;
    public string  codigo_tipo_empresa_destino                  { get; set; } = string.Empty;
    public string  codigo_empresa_destino                       { get; set; } = string.Empty;
    public string  codigo_dependencia_destino                   { get; set; } = string.Empty;
    public string? indicador_status_estorno                     { get; set; }
    public string  sigla_sistema_evento                         { get; set; } = string.Empty;
    public int?    numero_parcela_contrato                      { get; set; }
    public int?    numero_prazo_cota                            { get; set; }
    public string? data_vencimento_parcela_contrato             { get; set; }
    public string  data_contabil_transacao                      { get; set; } = string.Empty;
    public string? indicador_tributacao_imposto                 { get; set; }
    public double? valor_fundo_comum                            { get; set; }
    public double? valor_fundo_reserva                          { get; set; }
    public double? valor_taxa_administracao_paga                { get; set; }
    public double? valor_seguro_pagar                           { get; set; }
    public double? valor_multa_juro_pagar                       { get; set; }
    public double? valor_multa_juro_administradora              { get; set; }
    public double? valor_outro                                  { get; set; }
    public double? valor_total_grupo                            { get; set; }
    public double? valor_total_demais_parcela                   { get; set; }
    public double  valor_total_lancamento                       { get; set; }
    public double? valor_total_tributacao                       { get; set; }
    public string? indicador_tarifa_movimento                   { get; set; }
    public string? data_operacao_origem                         { get; set; }
    public string? codigo_tipo_motivo_estorno                   { get; set; }
    public string? codigo_unico_transacao_origem                { get; set; }
    public string  codigo_sistema_integrador                    { get; set; } = string.Empty;
    public string? indicador_grupo_antes_lei                    { get; set; }
}
