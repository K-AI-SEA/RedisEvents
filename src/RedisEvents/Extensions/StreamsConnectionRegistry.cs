using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Tracing;

namespace RedisEvents.Extensions;

/// <summary>
/// Every streams connection a process uses, and which topic lives on which: the default connection
/// (<c>Streams:ConnectionString</c>) plus one provider per distinct server among
/// <c>Streams:Connections</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>One multiplexer per server, not per name.</b> Named connections that reach the default
/// connection's server share the default provider; named connections that reach each other's server
/// share one provider between them. Pointing every name at one Redis therefore opens exactly the
/// connections single-server mode opens. "Same server" is decided by
/// <see cref="RedisServerIdentity"/>: a shared endpoint as written, or as resolved by DNS.
/// </para>
/// <para>
/// <b>A topic's keys all follow the topic.</b> Every per-topic path — publishers, state stores, the
/// consumer host with its positions, reset markers, ownership and reader, the trimmer and the lag
/// sampler — asks <see cref="ForTopic"/>. What is not per topic (views, a service's own state,
/// <see cref="StreamsConnection.GetSharedDatabase"/>) stays on <see cref="Default"/>.
/// </para>
/// <para>
/// With no <c>Streams:Connections</c> section this holds the default provider alone and every topic
/// resolves to it: single-server mode, unchanged.
/// </para>
/// </remarks>
internal sealed class StreamsConnectionRegistry : IDisposable, IAsyncDisposable
{
    private readonly StreamOptions options;
    private readonly Dictionary<string, StreamsConnectionProvider> byName = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<StreamsConnectionProvider> owned = [];
    private readonly List<StreamsConnectionProvider> distinct = [];
    private readonly HashSet<StreamsConnectionProvider> used = [];
    private readonly Lock gate = new();
    private bool disposed;

    /// <summary>
    /// Builds the registry. Named connections are grouped by server here, once; nothing connects until
    /// a provider's <see cref="StreamsConnectionProvider.Connection"/> is first read.
    /// </summary>
    /// <param name="options">The bound, validated options.</param>
    /// <param name="defaultProvider">The default connection's provider, registered in the container.</param>
    /// <param name="services">The container, for the reuse-or-connect check of each named provider.</param>
    /// <param name="logger">Optional logger; the name-to-server grouping is logged at Information.</param>
    /// <param name="tracingApplier">Optional tracing applier for the named multiplexers.</param>
    /// <exception cref="StreamConfigurationException">Two names reach one server but select different databases.</exception>
    public StreamsConnectionRegistry(
        StreamOptions options,
        StreamsConnectionProvider defaultProvider,
        IServiceProvider? services = null,
        ILogger? logger = null,
        IRedisTracingApplier? tracingApplier = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(defaultProvider);

        this.options = options;
        this.Default = defaultProvider;
        this.distinct.Add(defaultProvider);

        // Ordered by name so the grouping, and which name a shared provider is created for, does not
        // depend on the order configuration happened to enumerate in.
        foreach (var (name, connectionString) in (options.Connections ?? []).OrderBy(c => c.Key, StringComparer.Ordinal))
        {
            var match = this.distinct.FirstOrDefault(p => RedisServerIdentity.SameServer(p.ConnectionString, connectionString));

            if (match is not null)
            {
                RequireSameDatabase(name, connectionString, match);
                this.byName[name] = match;

                logger?.LogInformation(
                    "Streams: connection '{Name}' ({Endpoint}) is the same server as {Shared}; it shares that multiplexer.",
                    name,
                    string.Join(",", RedisServerIdentity.Written(connectionString)),
                    match.Name is null ? "the default connection" : $"connection '{match.Name}'");

                continue;
            }

            var provider = new StreamsConnectionProvider(options, services, logger, tracingApplier, name, connectionString);
            this.byName[name] = provider;
            this.owned.Add(provider);
            this.distinct.Add(provider);

            logger?.LogInformation(
                "Streams: connection '{Name}' ({Endpoint}) is a separate server; it gets a multiplexer of its own.",
                name,
                string.Join(",", RedisServerIdentity.Written(connectionString)));
        }
    }

    /// <summary>The default connection, <c>Streams:ConnectionString</c>.</summary>
    public StreamsConnectionProvider Default { get; }

    /// <summary>One provider per distinct server, the default first.</summary>
    public IReadOnlyList<StreamsConnectionProvider> Distinct => this.distinct;

    /// <summary>
    /// The providers some topic has been resolved to (and the default, always), for the health check:
    /// a named server nothing in the process uses is not this process's to report on.
    /// </summary>
    public IReadOnlyList<StreamsConnectionProvider> InUse
    {
        get
        {
            lock (this.gate)
            {
                return [.. this.distinct.Where(p => ReferenceEquals(p, this.Default) || this.used.Contains(p))];
            }
        }
    }

    /// <summary>
    /// The provider a topic lives on: its <see cref="TopicOptions.Connection"/>, or the default.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <returns>The provider every key of the topic goes through.</returns>
    /// <exception cref="StreamConfigurationException">The topic names a connection that is not configured.</exception>
    public StreamsConnectionProvider ForTopic(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var provider = this.ForName(StreamConfigBinder.NamedConnectionOf(this.options, topic));

        lock (this.gate)
        {
            this.used.Add(provider);
        }

        return provider;
    }

    /// <summary>
    /// The provider a topic lives on, out of a container: the registry's answer, or the lone
    /// <see cref="StreamsConnectionProvider"/> when a container was composed by hand without one.
    /// </summary>
    /// <param name="services">The built container.</param>
    /// <param name="topic">The topic name.</param>
    /// <returns>The provider every key of the topic goes through.</returns>
    public static StreamsConnectionProvider ForTopic(IServiceProvider services, string topic)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.GetService(typeof(StreamsConnectionRegistry)) is StreamsConnectionRegistry registry
            ? registry.ForTopic(topic)
            : (StreamsConnectionProvider?)services.GetService(typeof(StreamsConnectionProvider))
                ?? throw new StreamConfigurationException(
                    $"Streams: no streams connection is registered, so topic '{topic}' has nowhere to live. " +
                    "It is registered by the first AddStream/AddStreamPublisher/AddStreamStore call on the host builder.");
    }

    /// <summary>
    /// The provider for a connection name, or the default for <see langword="null"/>.
    /// </summary>
    /// <param name="name">A <see cref="StreamOptions.Connections"/> name, or <see langword="null"/>.</param>
    /// <returns>The provider.</returns>
    /// <exception cref="StreamConfigurationException">The name is not configured.</exception>
    public StreamsConnectionProvider ForName(string? name)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (string.IsNullOrWhiteSpace(name))
            return this.Default;

        if (this.byName.TryGetValue(name, out var provider))
            return provider;

        // Throws with the configured names listed.
        _ = StreamConfigBinder.NamedConnectionString(this.options, name);
        throw new StreamConfigurationException($"Streams: connection '{name}' is not configured.");
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        // The default provider belongs to the container; only the named ones are this registry's.
        foreach (var provider in this.owned)
            provider.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        foreach (var provider in this.owned)
            await provider.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// One multiplexer serves one database by default, so two names sharing a server must agree on
    /// it; otherwise the second name's keys would silently land in the first one's database.
    /// </summary>
    private static void RequireSameDatabase(string name, string connectionString, StreamsConnectionProvider shared)
    {
        var mine = RedisServerIdentity.DefaultDatabase(connectionString) ?? 0;
        var theirs = RedisServerIdentity.DefaultDatabase(shared.ConnectionString) ?? 0;

        if (mine == theirs)
            return;

        throw new StreamConfigurationException(
            $"{StreamConfigBinder.SectionName}:Connections:{name} reaches the same Redis server as " +
            $"{(shared.Name is null ? $"{StreamConfigBinder.SectionName}:ConnectionString" : $"{StreamConfigBinder.SectionName}:Connections:{shared.Name}")} " +
            $"but selects database {mine} where that one selects {theirs}. Names on one server share one multiplexer, so they must " +
            "select the same database. Use one database, or separate servers.");
    }
}
