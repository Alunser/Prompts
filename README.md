# KafkaProducer - RecebimentoMovimentoContabil

Producer console .NET 8 para publicar no tópico `emprestimos-e-financiamentos-recebimentos-movimento-contabil`.

## Pré-requisitos
- .NET 8 SDK
- Certificados `caroot.crt` e `FX90008.p12` na pasta `./certs/`

## Configuração

Edite o `appsettings.json` conforme necessário:

```json
{
  "KafkaConfiguration": {
    "ClientUser": "FX90008",
    "P12Location": "./certs/FX90008.p12",
    "P12Password": "SUA_SENHA_AQUI",
    "CaCertLocation": "./certs/caroot.crt"
  }
}
```

## Estrutura de pastas esperada

```
KafkaProducer/
├── certs/
│   ├── caroot.crt
│   └── FX90008.p12
├── appsettings.json
├── Program.cs
└── KafkaProducer.csproj
```

## Executar

```bash
dotnet restore
dotnet run
```

## Opções do menu

| Opção | Descrição |
|-------|-----------|
| 1     | Envia uma mensagem de exemplo preenchida automaticamente |
| 2     | Envia mensagem a partir de um JSON colado no terminal |
| 3     | Envia N mensagens em lote (bulk) |
| Q     | Encerra o producer com flush |

## Exemplo de JSON para opção 2

```json
{
  "codigo_identificacao_movimentacao_financeira": "MOVFIN-20260527-0001",
  "codigo_pessoa_corporativo": "12345678901",
  "codigo_tipo_pessoa_titular_recebivel": "F",
  "numero_centro_custo_debito": "CC001",
  "numero_centro_custo_credito": "CC002",
  "codigo_identificador_referencia_movimento": "REF-000001",
  "codigo_produto_operacional": 100,
  "identificador_evento_negocio": 1748346000000,
  "codigo_empresa": 341,
  "numero_grupo_bem_produto_consorcio": "GRP-0042",
  "numero_cota_consorcio": 42,
  "numero_sequencial_versao": 1,
  "numero_contrato": 9876543210,
  "codigo_tipo_empresa_origem": "341",
  "codigo_empresa_origem": "0001",
  "codigo_dependencia_origem": "0001",
  "codigo_tipo_empresa_destino": "341",
  "codigo_empresa_destino": "0002",
  "codigo_dependencia_destino": "0002",
  "sigla_sistema_evento": "CONS",
  "data_contabil_transacao": "2026-05-27",
  "valor_total_lancamento": 1755.00,
  "codigo_sistema_integrador": "FX9"
}
```
