using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaProducer.Configuration;
using KafkaProducer.Models;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

// ─── Configuração ───────────────────────────────────────────────────────────
var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var kafkaSettings    = config.GetSection("KafkaConfiguration").Get<KafkaSettings>()!;
var schemaSettings   = config.GetSection("SchemaRegistryConfiguration").Get<SchemaRegistrySettings>()!;
var producerSettings = config.GetSection("ProducerConfiguration").Get<ProducerSettings>()!;

// ─── Banner ──────────────────────────────────────────────────────────────────
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

Log($"Broker       : {producerSettings.BrokerUrl}");
Log($"Tópico       : {producerSettings.TopicName}");
Log($"Schema Reg.  : {schemaSettings.Url}");
Log($"ClientId     : {producerSettings.ClientId}");
Log($"Certificado  : {kafkaSettings.P12Location}");
Console.WriteLine();

// ─── Schema Registry ────────────────────────────────────────────────────────
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url                         = schemaSettings.Url,
    RequestTimeoutMs            = schemaSettings.RequestTimeoutMs,
    MaxCachedSchemas            = schemaSettings.MaxCachedSchemas,
    EnableSslCertificateVerification = schemaSettings.EnableSslCertificateVerification,
};

// ─── Producer Config ─────────────────────────────────────────────────────────
var producerConfig = new ProducerConfig
{
    BootstrapServers            = producerSettings.BrokerUrl,
    ClientId                    = producerSettings.ClientId,
    CompressionType             = CompressionType.Gzip,
    BatchNumMessages            = producerSettings.BatchNumMessages,
    LingerMs                    = producerSettings.LingerMs,
    Acks                        = Acks.All,
    SecurityProtocol            = SecurityProtocol.Ssl,
    SslCaLocation               = kafkaSettings.CaCertLocation,
    SslKeystoreLocation         = kafkaSettings.P12Location,
    SslKeystorePassword         = kafkaSettings.P12Password,
    EnableSslCertificateVerification = false,
};

// ─── Loop Principal ──────────────────────────────────────────────────────────
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
            await EnviarMensagemExemplo(producer, producerSettings.TopicName);
            break;

        case "2":
            await EnviarMensagemCustomizada(producer, producerSettings.TopicName);
            break;

        case "3":
            await EnviarEmLote(producer, producerSettings.TopicName);
            break;

        case "Q":
            LogSuccess("Encerrando producer...");
            producer.Flush(TimeSpan.FromSeconds(10));
            return;

        default:
            LogError("Opção inválida.");
            break;
    }

    Console.WriteLine();
}

// ─── Funções ─────────────────────────────────────────────────────────────────

async Task EnviarMensagemExemplo(IProducer<string, RecebimentoMovimentoContabil> prod, string topic)
{
    var mensagem = CriarMensagemExemplo();
    var key      = mensagem.data.codigo_identificacao_movimentacao_financeira;

    try
    {
        Log($"Publicando mensagem com key: {key}");
        var result = await prod.ProduceAsync(topic, new Message<string, RecebimentoMovimentoContabil>
        {
            Key   = key,
            Value = mensagem
        });
        LogSuccess($"✔ Publicado → Partition: {result.Partition.Value} | Offset: {result.Offset.Value}");
    }
    catch (Exception ex)
    {
        LogError($"Erro ao publicar: {ex.Message}");
    }
}

async Task EnviarMensagemCustomizada(IProducer<string, RecebimentoMovimentoContabil> prod, string topic)
{
    Console.WriteLine("Cole o JSON da mensagem (RecebimentoMovimentoContabilData) e pressione Enter duas vezes:");
    var sb = new System.Text.StringBuilder();
    string? line;
    while (!string.IsNullOrWhiteSpace(line = Console.ReadLine()))
        sb.AppendLine(line);

    try
    {
        var data     = JsonSerializer.Deserialize<RecebimentoMovimentoContabilData>(sb.ToString())!;
        var mensagem = new RecebimentoMovimentoContabil { data = data };
        var key      = data.codigo_identificacao_movimentacao_financeira;

        var result = await prod.ProduceAsync(topic, new Message<string, RecebimentoMovimentoContabil>
        {
            Key   = key,
            Value = mensagem
        });
        LogSuccess($"✔ Publicado → Partition: {result.Partition.Value} | Offset: {result.Offset.Value}");
    }
    catch (JsonException jex)
    {
        LogError($"JSON inválido: {jex.Message}");
    }
    catch (Exception ex)
    {
        LogError($"Erro ao publicar: {ex.Message}");
    }
}

async Task EnviarEmLote(IProducer<string, RecebimentoMovimentoContabil> prod, string topic)
{
    Console.Write("Quantas mensagens enviar? ");
    if (!int.TryParse(Console.ReadLine(), out var qtd) || qtd <= 0)
    {
        LogError("Quantidade inválida.");
        return;
    }

    Log($"Enviando {qtd} mensagens em lote...");
    var tasks = new List<Task<DeliveryResult<string, RecebimentoMovimentoContabil>>>();

    for (int i = 1; i <= qtd; i++)
    {
        var mensagem = CriarMensagemExemplo(i);
        var key      = mensagem.data.codigo_identificacao_movimentacao_financeira;

        tasks.Add(prod.ProduceAsync(topic, new Message<string, RecebimentoMovimentoContabil>
        {
            Key   = key,
            Value = mensagem
        }));
    }

    try
    {
        var results = await Task.WhenAll(tasks);
        LogSuccess($"✔ {results.Length} mensagens publicadas com sucesso.");

        foreach (var r in results)
            Console.WriteLine($"   → Key: {r.Key} | Partition: {r.Partition.Value} | Offset: {r.Offset.Value}");
    }
    catch (Exception ex)
    {
        LogError($"Erro no lote: {ex.Message}");
    }
}

RecebimentoMovimentoContabil CriarMensagemExemplo(int seq = 1) => new()
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
        data_contemplacao_consorcio                 = null,
        data_entrega_bem_consorcio                  = null,
        codigo_situacao_cobranca                    = "A",
        data_cancelamento_cota                      = null,
        codigo_situacao_grupo                       = "A",
        codigo_tipo_empresa_origem                  = "341",
        codigo_empresa_origem                       = "0001",
        codigo_dependencia_origem                   = "0001",
        codigo_tipo_empresa_destino                 = "341",
        codigo_empresa_destino                      = "0002",
        codigo_dependencia_destino                  = "0002",
        indicador_status_estorno                    = null,
        sigla_sistema_evento                        = "CONS",
        numero_parcela_contrato                     = 1,
        numero_prazo_cota                           = 60,
        data_vencimento_parcela_contrato            = DateTime.Now.AddMonths(1).ToString("yyyy-MM-dd"),
        data_contabil_transacao                     = DateTime.Now.ToString("yyyy-MM-dd"),
        indicador_tributacao_imposto                = null,
        valor_fundo_comum                           = 1500.00,
        valor_fundo_reserva                         = 150.00,
        valor_taxa_administracao_paga               = 75.00,
        valor_seguro_pagar                          = 30.00,
        valor_multa_juro_pagar                      = null,
        valor_multa_juro_administradora             = null,
        valor_outro                                 = null,
        valor_total_grupo                           = 5000.00,
        valor_total_demais_parcela                  = null,
        valor_total_lancamento                      = 1755.00,
        valor_total_tributacao                      = null,
        indicador_tarifa_movimento                  = null,
        data_operacao_origem                        = DateTime.Now.ToString("yyyy-MM-dd"),
        codigo_tipo_motivo_estorno                  = null,
        codigo_unico_transacao_origem               = null,
        codigo_sistema_integrador                   = "FX9",
        indicador_grupo_antes_lei                   = null
    }
};

void Log(string msg)
{
    Console.ForegroundColor = ConsoleColor.Gray;
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");
    Console.ResetColor();
}

void LogSuccess(string msg)
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {msg}");
    Console.ResetColor();
}

void LogError(string msg)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] ✖ {msg}");
    Console.ResetColor();
}
