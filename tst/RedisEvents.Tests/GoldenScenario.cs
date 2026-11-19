using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Config;
using RedisEvents.Extensions;
using RedisEvents.Producer;

namespace RedisEvents.Tests;

/// <summary>
/// One fixed workload, run against one server, that touches every kind of key the library keeps for
/// a topic: partition streams and topic metadata (a publisher), positions, ownership and a blocking
/// reader (a Block consumer), a second consumer in Poll mode, a state stream (a stream store), and an
/// outbox state key. What it leaves behind, and how many connections it holds open while running,
/// are compared against the golden values recorded from the release before named connections.
/// </summary>
internal static class GoldenScenario
{
    /// <summary>
    /// Stands in for the per-run topic prefix in the reported keys. Each run takes fresh topic names
    /// because the library remembers, per process, which topics it has already reconciled, and a
    /// second run on the same names would skip writing their metadata keys.
    /// </summary>
    private const string Placeholder = "golden";

    private static int runs;

    /// <summary>
    /// Every key the scenario leaves on the server, from the release before named connections
    /// (RedisEvents 04f754a, single-server mode, <c>STREAMS_HASH_TAG</c> unset). Named connections must not change a key name, so
    /// both the no-<c>Connections</c> run and the all-names-on-one-server run must match it exactly.
    /// </summary>
    public static readonly string[] GoldenKeys =
    [
        "re:m:{golden.ledger}",
        "re:m:{golden.orders}",
        "re:p:{golden.ledger}:golden-svc",
        "re:p:{golden.ledger}:golden-svc:meta",
        "re:p:{golden.orders}:golden-svc",
        "re:p:{golden.orders}:golden-svc:meta",
        "re:s:{golden.ledger}:0",
        "re:s:{golden.orders}:0",
        "re:s:{golden.orders}:1",
        "re:{golden.ledger}:state:balance:1",
        "re:{golden.ledger}:state:es:Account:1",
    ];

    /// <summary>The keyspace while the scenario runs, on the same release: the above plus the two ownership hashes.</summary>
    public static readonly string[] GoldenRunningKeys =
    [
        "re:m:{golden.ledger}",
        "re:m:{golden.orders}",
        "re:o:{golden.ledger}:golden-svc",
        "re:o:{golden.orders}:golden-svc",
        "re:p:{golden.ledger}:golden-svc",
        "re:p:{golden.ledger}:golden-svc:meta",
        "re:p:{golden.orders}:golden-svc",
        "re:p:{golden.orders}:golden-svc:meta",
        "re:s:{golden.ledger}:0",
        "re:s:{golden.orders}:0",
        "re:s:{golden.orders}:1",
        "re:{golden.ledger}:state:balance:1",
        "re:{golden.ledger}:state:es:Account:1",
    ];

    /// <summary>
    /// Client connections the scenario holds open on the server while it runs, on the same release:
    /// the shared multiplexer, one high-integrity ownership multiplexer per consumer, and the Block
    /// consumer's reader — each a multiplexer of two connections (interactive and subscription).
    /// </summary>
    public const int GoldenClients = 8;

    /// <summary>Runs the scenario on a flushed server and reports the keys left and the connections held.</summary>
    /// <param name="server">The server; flushed first.</param>
    /// <param name="extra">
    /// Extra configuration, layered over the scenario's own (named connections, say). <c>{orders}</c>
    /// and <c>{ledger}</c> in a key stand for this run's two topic names.
    /// </param>
    /// <returns>The sorted keyspace after the host stopped and while it ran, and the client count while it ran.</returns>
    public static async Task<(string[] Keys, string[] RunningKeys, int Clients)> RunAsync(TwoRedisFixture.Server server, IDictionary<string, string?>? extra = null)
    {
        var prefix = $"golden{Interlocked.Increment(ref runs)}x{Environment.ProcessId}";
        var ordersTopic = $"{prefix}.orders";
        var ledgerTopic = $"{prefix}.ledger";

        await server.FlushAsync();
        await RedisStreamsFixture.WaitUntilAsync(
            async () => await server.ClientCountAsync() == 0,
            TimeSpan.FromSeconds(20),
            "connections of earlier tests to close");

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });

        var config = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Streams:ConnectionString"] = server.ConnectionString,
            ["Streams:Consumer"] = "golden-svc",
            [$"Streams:Topics:{ordersTopic}:Partitions"] = "2",
            [$"Streams:Topics:{ledgerTopic}:Partitions"] = "1",
            ["Streams:Consumers:0:Topic"] = ordersTopic,
            ["Streams:Consumers:0:ReadMode"] = "Block",
            ["Streams:Consumers:0:BlockMs"] = "200",
            ["Streams:Consumers:0:PersistIntervalMs"] = "100",
            ["Streams:Consumers:1:Topic"] = ledgerTopic,
            ["Streams:Consumers:1:ReadMode"] = "Poll",
            ["Streams:Consumers:1:PersistIntervalMs"] = "100",
        };

        foreach (var (key, value) in extra ?? new Dictionary<string, string?>())
            config[key.Replace("{orders}", ordersTopic, StringComparison.Ordinal).Replace("{ledger}", ledgerTopic, StringComparison.Ordinal)] = value;

        builder.Configuration.AddInMemoryCollection(config);

        var orders = 0;
        var ledger = 0;

        builder.AddStreamPublisher(ordersTopic);
        builder.AddStreamStore(ledgerTopic);
        builder.AddStream(ordersTopic, (batch, _) =>
        {
            Interlocked.Add(ref orders, batch.Length);
            return ValueTask.CompletedTask;
        });
        builder.AddStream(ledgerTopic, (batch, _) =>
        {
            Interlocked.Add(ref ledger, batch.Length);
            return ValueTask.CompletedTask;
        });

        using var host = builder.Build();
        await host.StartAsync();

        int clients;
        string[] running;
        try
        {
            var publisher = host.Services.GetRequiredService<IStreamPublisher>();
            foreach (var key in new[] { "k1", "k2", "k3" })
                await publisher.PublishAsync(key, Encoding.UTF8.GetBytes(key), "golden.ordered");

            var store = host.Services.GetRequiredService<IStreamStore>();
            _ = await store.AppendAndPublishAsync(
                "es:Account:1",
                expectedLength: 0,
                "1",
                [new StateEvent(Encoding.UTF8.GetBytes("opened"), "golden.opened")]);

            // The service's own database (the default connection) writing the ledger topic's outbox.
            var options = host.Services.GetRequiredService<StreamOptions>();
            var id = await Outbox.WriteAndPublishAsync(
                StreamsConnection.GetSharedDatabase(host.Services),
                tran => _ = tran.StringSetAsync(Outbox.StateKey(ledgerTopic, "balance:1"), "10"),
                ledgerTopic,
                "1",
                Encoding.UTF8.GetBytes("credited"),
                "golden.credited",
                stateKeys: [Outbox.StateKey(ledgerTopic, "balance:1")],
                topicOptions: options.Topics[ledgerTopic]);

            if (id is null)
                throw new InvalidOperationException("The golden outbox write had no condition and still returned null.");

            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref orders) >= 3 && Volatile.Read(ref ledger) >= 2,
                TimeSpan.FromSeconds(30),
                "both consumers to see every golden message");

            // Let positions flush and every connection settle before counting them.
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            clients = await server.ClientCountAsync();
            running = await server.KeysAsync();
        }
        finally
        {
            await host.StopAsync();
        }

        string[] Normalise(string[] keys) => [.. keys.Select(k => k.Replace(prefix, Placeholder, StringComparison.Ordinal))];
        return (Normalise(await server.KeysAsync()), Normalise(running), clients);
    }
}
