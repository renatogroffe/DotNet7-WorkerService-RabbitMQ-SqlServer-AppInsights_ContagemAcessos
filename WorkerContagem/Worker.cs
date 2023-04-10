using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WorkerContagem.Data;
using WorkerContagem.Models;

namespace WorkerContagem;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly ContagemRepository _repository;
    private readonly TelemetryConfiguration _telemetryConfig;
    private readonly string _queueName;
    private readonly int _intervaloMensagemWorkerAtivo;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        ContagemRepository repository,
        TelemetryConfiguration telemetryConfig)
    {
        _logger = logger;
        _configuration = configuration;
        _repository = repository;
        _telemetryConfig = telemetryConfig;

        _queueName = _configuration["RabbitMQ:Queue"]!;
        _intervaloMensagemWorkerAtivo =
            Convert.ToInt32(configuration["IntervaloMensagemWorkerAtivo"]);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Queue = {_queueName}");
        _logger.LogInformation("Aguardando mensagens...");

        var factory = new ConnectionFactory()
        {
            Uri = new Uri(_configuration.GetConnectionString("RabbitMQ")!)
        };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(queue: _queueName,
                            durable: false,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += ProcessarResultado;
        channel.BasicConsume(queue: _queueName,
            autoAck: true,
            consumer: consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                $"Worker ativo em: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            await Task.Delay(_intervaloMensagemWorkerAtivo, stoppingToken);
        }
    }

    private void ProcessarResultado(
        object? sender, BasicDeliverEventArgs e)
    {
        var start = DateTime.Now;
        var watch = new Stopwatch();
        watch.Start();

        var messageContent = Encoding.UTF8.GetString(e.Body.ToArray());
        _logger.LogInformation(
            $"[{_queueName} | Nova mensagem] " + messageContent);

        watch.Stop();
        TelemetryClient client = new(_telemetryConfig);
        client.TrackDependency(
            "RabbitMQ", $"Consume {_queueName}",
            messageContent, start, watch.Elapsed, true);

        ResultadoContador? resultado;
        try
        {
            resultado = JsonSerializer.Deserialize<ResultadoContador>(messageContent,
                new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });
        }
        catch
        {
            _logger.LogError("Dados inválidos para o Resultado");
            resultado = null;
        }

        if (resultado is not null)
        {
            try
            {
                _repository.Save(resultado);
                _logger.LogInformation("Resultado registrado com sucesso!");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro durante a gravação: {ex.Message}");
            }
        }
    }
}