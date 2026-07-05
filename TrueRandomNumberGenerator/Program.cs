using System.IO.Ports;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddSingleton(Channel.CreateBounded<byte>(new BoundedChannelOptions(128_000)
{
    FullMode = BoundedChannelFullMode.DropWrite,
    SingleWriter = true,
    SingleReader = false
}));

builder.Services.Configure<SerialHardwareOptions>(builder.Configuration.GetSection("SerialHardware"));

builder.Services.AddSingleton<SerialConnectionManager>();
builder.Services.AddSingleton<SerialAsciiNumberParser>();
builder.Services.AddSingleton<EntropyHealthMonitor>();
builder.Services.AddSingleton<VonNeumannExtractor>();
builder.Services.AddSingleton<Sha256EntropyPool>();

builder.Services.AddHostedService<ColetorEntropiaWorker>();

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default);
});

var app = builder.Build();

app.MapGet("/api/entropia", async (
    [FromQuery] int quantidade,
    [FromServices] Channel<byte> channel,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    if (quantidade <= 0 || quantidade > 4096)
        return Results.BadRequest("Quantidade deve ser entre 1 e 4096.");

    var reader = channel.Reader;
    var bufferSaida = new byte[quantidade];

    logger.LogInformation(
        "Requisição de {Quantidade} bytes de entropia. Disponíveis no buffer: {Disponiveis}.",
        quantidade,
        reader.Count);

    try
    {
        for (var i = 0; i < quantidade; i++)
        {
            if (!reader.TryRead(out bufferSaida[i]))
            {
                bufferSaida[i] = await reader.ReadAsync(cancellationToken);
            }
        }

        var hexResult = Convert.ToHexString(bufferSaida);
        return Results.Ok(new RespostaEntropia(hexResult));
    }
    finally
    {
        CryptographicOperations.ZeroMemory(bufferSaida);
    }
});

app.MapGet("/api/status", (
    SerialConnectionManager manager,
    Channel<byte> channel) =>
{
    return Results.Ok(new StatusEntropiaResponse(
        manager.EstaConectado,
        channel.Reader.Count));
});

app.Logger.LogInformation("=== API de Entropia Física por Configuração Fixa iniciada ===");

app.Run();

/// <summary>
/// Opções de configuração para o mapeamento e conexão com o hardware serial, compatível com Native AOT.
/// </summary>
public sealed class SerialHardwareOptions
{
    /// <summary>
    /// Identificador da porta serial (ex: COM3 ou /dev/ttyUSB0).
    /// </summary>
    public string Porta { get; set; } = string.Empty;

    /// <summary>
    /// Taxa de transmissão de dados em bits por segundo.
    /// </summary>
    public int BaudRate { get; set; } = 115200;
}

/// <summary>
/// Representa a resposta contendo a string de entropia gerada em formato hexadecimal.
/// </summary>
public record RespostaEntropia(string Entropy);

/// <summary>
/// Representa o estado atual do serviço de coleta e a quantidade de bytes disponíveis no pool.
/// </summary>
public record StatusEntropiaResponse(bool SerialConectada, int BytesDisponiveis);

/// <summary>
/// Contexto de serialização JSON otimizado por Source Generators para suporte a compilação nativa (Native AOT).
/// </summary>
[JsonSerializable(typeof(RespostaEntropia))]
[JsonSerializable(typeof(StatusEntropiaResponse))]
[JsonSerializable(typeof(SerialHardwareOptions))]
public partial class AppJsonContext : JsonSerializerContext
{
}

/// <summary>
/// Gerenciador de estado thread-safe responsável por monitorar a conectividade da porta serial.
/// </summary>
public sealed class SerialConnectionManager
{
    private readonly object _lock = new();
    private bool _estaConectado;

    /// <summary>
    /// Indica se a conexão ativa com o hardware serial está estabelecida.
    /// </summary>
    public bool EstaConectado
    {
        get
        {
            lock (_lock) return _estaConectado;
        }
    }

    /// <summary>
    /// Atualiza o estado de conectividade atual da porta serial de forma sincronizada.
    /// </summary>
    public void SetConectado(bool conectado)
    {
        lock (_lock) _estaConectado = conectado;
    }
}

/// <summary>
/// Serviço de segundo plano encarregado da ingestão de fluxos binários, tratamento de exceções, 
/// processamento estatístico e publicação assíncrona de entropia.
/// </summary>
public sealed class ColetorEntropiaWorker : BackgroundService
{
    private readonly Channel<byte> _canal;
    private readonly SerialConnectionManager _connectionManager;
    private readonly SerialAsciiNumberParser _parser;
    private readonly EntropyHealthMonitor _healthMonitor;
    private readonly VonNeumannExtractor _vonNeumannExtractor;
    private readonly Sha256EntropyPool _entropyPool;
    private readonly ILogger<ColetorEntropiaWorker> _logger;
    private readonly SerialHardwareOptions _options;
    private readonly byte[] _serialReadBuffer = new byte[512];

    /// <summary>
    /// Inicializa uma nova instância do coletor injetando suas dependências de pipeline e configurações.
    /// </summary>
    public ColetorEntropiaWorker(
        Channel<byte> canal,
        SerialConnectionManager connectionManager,
        SerialAsciiNumberParser parser,
        EntropyHealthMonitor healthMonitor,
        VonNeumannExtractor vonNeumannExtractor,
        Sha256EntropyPool entropyPool,
        Microsoft.Extensions.Options.IOptions<SerialHardwareOptions> options,
        ILogger<ColetorEntropiaWorker> logger)
    {
        _canal = canal;
        _connectionManager = connectionManager;
        _parser = parser;
        _healthMonitor = healthMonitor;
        _vonNeumannExtractor = vonNeumannExtractor;
        _entropyPool = entropyPool;
        _logger = logger;
        _options = options.Value;
    }

    /// <summary>
    /// Executa o loop de execução principal assíncrono para conexão, leitura física e processamento de dados.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Iniciando fluxo de leitura fixa da porta: {Porta} a {BaudRate} bps.", _options.Porta, _options.BaudRate);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var port = new SerialPort(_options.Porta, _options.BaudRate)
                {
                    ReadTimeout = 1500,
                    WriteTimeout = 1500,
                    DtrEnable = true,
                    RtsEnable = true
                };

                port.Open();
                _connectionManager.SetConectado(true);
                _logger.LogInformation("Conexão serial estabelecida na porta {Porta}.", _options.Porta);

                ResetarEstadoInterno();
                var stream = port.BaseStream;

                while (!stoppingToken.IsCancellationRequested)
                {
                    int bytesLidos = await stream.ReadAsync(_serialReadBuffer.AsMemory(), stoppingToken);
                    if (bytesLidos == 0) continue;

                    for (var i = 0; i < bytesLidos; i++)
                    {
                        var b = _serialReadBuffer[i];
                        if (!_parser.TryProcessarByte(b, out var valorAnalogico))
                            continue;

                        ProcessarValorAnalogico(valorAnalogico);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _connectionManager.SetConectado(false);
                _logger.LogError(ex, "Falha ou desconexão da porta {Porta}. Aguardando 5 segundos para tentar reconectar...", _options.Porta);

                try
                {
                    await Task.Delay(5000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            finally
            {
                _connectionManager.SetConectado(false);
            }
        }
    }

    private void ProcessarValorAnalogico(int valorAnalogico)
    {
        if (valorAnalogico is < 0 or > 4095) return;

        if (!_healthMonitor.AmostraEhSaudavel(valorAnalogico)) return;

        var bitBruto = valorAnalogico & 1;

        if (!_vonNeumannExtractor.TryExtrairBit(bitBruto, out var bitDesenviesado)) return;

        if (!_entropyPool.TryAdicionarBit(bitDesenviesado, out var hashExtraido)) return;

        PublicarHashNoCanal(hashExtraido);
    }

    private void PublicarHashNoCanal(byte[] hashExtraido)
    {
        try
        {
            var writer = _canal.Writer;
            foreach (var b in hashExtraido)
            {
                if (!writer.TryWrite(b)) break;
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(hashExtraido);
        }
    }

    private void ResetarEstadoInterno()
    {
        _parser.Reset();
        _healthMonitor.Reset();
        _vonNeumannExtractor.Reset();
        _entropyPool.Reset();
    }
}

/// <summary>
/// Máquina de estados focada em alocação zero para realizar o parsing de texto ASCII numérico 
/// originado de streams de bytes sequenciais terminados em quebras de linha.
/// </summary>
public sealed class SerialAsciiNumberParser
{
    private int _valorAcumulado;
    private bool _temDigito;

    /// <summary>
    /// Avalia um byte de forma incremental para reconstruir o número inteiro sem alocação de strings intermediárias.
    /// </summary>
    /// <param name="b">Byte bruto lido da stream de hardware.</param>
    /// <param name="valor">O valor numérico inteiro decodificado se o terminador de linha for atingido.</param>
    /// <returns>True caso um número completo tenha sido decodificado; caso contrário, False.</returns>
    public bool TryProcessarByte(byte b, out int valor)
    {
        valor = default;
        if (b is >= (byte)'0' and <= (byte)'9')
        {
            _valorAcumulado = (_valorAcumulado * 10) + (b - '0');
            _temDigito = true;
            return false;
        }
        if (b is not ((byte)'\r') and not ((byte)'\n')) return false;
        if (!_temDigito) return false;

        valor = _valorAcumulado;
        _valorAcumulado = 0;
        _temDigito = false;
        return true;
    }

    /// <summary>
    /// Restaura os acumuladores internos do parser para o estado inicial neutro.
    /// </summary>
    public void Reset() { _valorAcumulado = 0; _temDigito = false; }
}

/// <summary>
/// Monitor de saúde estatístico projetado para identificar comportamentos anômalos ou congelamento de sinal analógico.
/// </summary>
public sealed class EntropyHealthMonitor
{
    private const int MaxConsecutiveSameValue = 20;
    private int _ultimoValorBruto = -1;
    private int _repeticoesConsecutivas;

    /// <summary>
    /// Verifica se a amostra atual atende aos critérios mínimos de saúde estatística contra repetição excessiva de padrões idênticos.
    /// </summary>
    public bool AmostraEhSaudavel(int valorAtual)
    {
        if (valorAtual == _ultimoValorBruto)
        {
            _repeticoesConsecutivas++;
            return _repeticoesConsecutivas < MaxConsecutiveSameValue;
        }
        _ultimoValorBruto = valorAtual;
        _repeticoesConsecutivas = 0;
        return true;
    }

    /// <summary>
    /// Limpa os registros de amostragem históricos do monitor de integridade.
    /// </summary>
    public void Reset() { _ultimoValorBruto = -1; _repeticoesConsecutivas = 0; }
}

/// <summary>
/// Extrator matemático baseado no algoritmo de Von Neumann para eliminar viés em sequências binárias brutas.
/// </summary>
public sealed class VonNeumannExtractor
{
    private int _primeiroBit = -1;

    /// <summary>
    /// Processa pares de bits subsequentes desenviesando a distribuição estatística da amostragem física.
    /// </summary>
    public bool TryExtrairBit(int bitAtual, out int bitDesenviesado)
    {
        bitDesenviesado = default;
        if (bitAtual is not 0 and not 1) return false;

        if (_primeiroBit == -1)
        {
            _primeiroBit = bitAtual;
            return false;
        }
        var primeiro = _primeiroBit;
        _primeiroBit = -1;

        if (primeiro == bitAtual) return false;

        bitDesenviesado = primeiro;
        return true;
    }

    /// <summary>
    /// Limpa o estado temporário do acumulador de pares do extrator de bits.
    /// </summary>
    public void Reset() => _primeiroBit = -1;
}

/// <summary>
/// Pool de acumulação criptográfica responsável por agrupar bits desenviesados e processá-los com hashing SHA-256.
/// </summary>
public sealed class Sha256EntropyPool
{
    private readonly byte[] _pool = new byte[64];
    private int _poolIndex;
    private uint _bitBuffer;
    private int _bitCount;

    /// <summary>
    /// Adiciona um bit isolado ao buffer interno, empacotando-o em formato de bytes e disparando a compressão criptográfica por SHA-256 quando o pool estiver cheio.
    /// </summary>
    public bool TryAdicionarBit(int bit, out byte[] hashExtraido)
    {
        hashExtraido = Array.Empty<byte>();
        if (bit is not 0 and not 1) return false;

        _bitBuffer = (_bitBuffer << 1) | (uint)bit;
        _bitCount++;

        if (_bitCount < 8) return false;

        _pool[_poolIndex++] = (byte)(_bitBuffer & 0xFF);
        _bitBuffer = 0;
        _bitCount = 0;

        if (_poolIndex < _pool.Length) return false;

        hashExtraido = SHA256.HashData(_pool);
        ResetPoolOnly();
        return true;
    }

    /// <summary>
    /// Redefine todos os buffers e limpadores de memória associados ao pool criptográfico de entropia.
    /// </summary>
    public void Reset() { ResetPoolOnly(); _bitBuffer = 0; _bitCount = 0; }
    private void ResetPoolOnly() { _poolIndex = 0; CryptographicOperations.ZeroMemory(_pool); }
}