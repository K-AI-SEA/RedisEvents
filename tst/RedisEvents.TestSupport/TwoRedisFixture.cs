using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>The collection name of the tests that need two Redis servers.</summary>
/// <remarks>
/// Like <see cref="RedisStreamsCollection"/>, each consuming test assembly registers its own
/// <c>[CollectionDefinition(TwoRedisCollection.Name)] : ICollectionFixture&lt;TwoRedisFixture&gt;</c>.
/// Tests in one collection run one after another, so a test may flush both servers.
/// </remarks>
public static class TwoRedisCollection
{
    /// <summary>The collection name.</summary>
    public const string Name = "TwoRedis";
}

/// <summary>
/// Two independent throwaway Redis containers, <see cref="A"/> and <see cref="B"/>, for the tests
/// that prove a topic's keys follow its named connection and never land on the other server.
/// </summary>
/// <remarks>
/// Separate from <see cref="RedisStreamsFixture"/>, whose single container is shared by every other
/// service test: these tests flush both servers to compare whole keyspaces, which would race the
/// rest of the suite there.
/// </remarks>
public sealed class TwoRedisFixture : IAsyncLifetime
{
    private const int RedisPort = 6379;

    private IContainer? containerA;
    private IContainer? containerB;

    /// <summary>The first server.</summary>
    public Server A { get; private set; } = null!;

    /// <summary>The second server.</summary>
    public Server B { get; private set; } = null!;

    /// <summary>Flushes both servers.</summary>
    /// <returns>A task that completes when both are empty.</returns>
    public async Task FlushAllAsync()
    {
        await this.A.FlushAsync().ConfigureAwait(false);
        await this.B.FlushAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        var startA = StartAsync();
        var startB = StartAsync();

        (this.containerA, this.A) = await startA.ConfigureAwait(false);
        (this.containerB, this.B) = await startB.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.A is not null)
            await this.A.Admin.DisposeAsync().ConfigureAwait(false);

        if (this.B is not null)
            await this.B.Admin.DisposeAsync().ConfigureAwait(false);

        if (this.containerA is not null)
            await this.containerA.DisposeAsync().ConfigureAwait(false);

        if (this.containerB is not null)
            await this.containerB.DisposeAsync().ConfigureAwait(false);
    }

    private static async Task<(IContainer Container, Server Server)> StartAsync()
    {
        var container = new ContainerBuilder(RedisStreamsFixture.Image)
            .WithPortBinding(RedisPort, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilExternalTcpPortIsAvailable(RedisPort))
            .Build();

        await container.StartAsync().ConfigureAwait(false);

        var host = container.Hostname;
        var port = container.GetMappedPublicPort(RedisPort);
        var admin = await ConnectionMultiplexer
            .ConnectAsync($"{host}:{port},abortConnect=false,allowAdmin=true,name={AdminClientName}")
            .ConfigureAwait(false);

        return (container, new Server(host, port, admin));
    }

    /// <summary>The client name of each server's admin connection, so a connection count can leave it out.</summary>
    public const string AdminClientName = "two-redis-fixture";

    /// <summary>One of the two servers.</summary>
    /// <param name="Host">The host the container is reached on.</param>
    /// <param name="Port">The mapped port.</param>
    /// <param name="Admin">A connection with <c>allowAdmin</c>, for flushing, scanning and counting clients.</param>
    public sealed record Server(string Host, ushort Port, IConnectionMultiplexer Admin)
    {
        /// <summary><c>host:port</c>.</summary>
        public string Endpoint => $"{this.Host}:{this.Port}";

        /// <summary>A connection string for code under test (no <c>allowAdmin</c>).</summary>
        public string ConnectionString => $"{this.Endpoint},abortConnect=false";

        /// <summary>A database on the admin connection.</summary>
        public IDatabase Db => this.Admin.GetDatabase();

        /// <summary>Every key on the server, sorted ordinally.</summary>
        /// <returns>The keys.</returns>
        public async Task<string[]> KeysAsync()
        {
            var server = this.Admin.GetServer(this.Admin.GetEndPoints()[0]);
            var keys = new List<string>();

            await foreach (var key in server.KeysAsync(pageSize: 500).ConfigureAwait(false))
                keys.Add(key.ToString());

            keys.Sort(StringComparer.Ordinal);
            return [.. keys];
        }

        /// <summary>Client connections open on the server, other than this fixture's own admin connection.</summary>
        /// <returns>The number of clients.</returns>
        public async Task<int> ClientCountAsync()
        {
            var server = this.Admin.GetServer(this.Admin.GetEndPoints()[0]);
            var clients = await server.ClientListAsync().ConfigureAwait(false);
            return clients.Count(c => !string.Equals(c.Name, AdminClientName, StringComparison.Ordinal));
        }

        /// <summary>Empties the server.</summary>
        /// <returns>A task that completes when it is empty.</returns>
        public async Task FlushAsync()
        {
            var server = this.Admin.GetServer(this.Admin.GetEndPoints()[0]);
            await server.FlushAllDatabasesAsync().ConfigureAwait(false);
        }
    }
}
