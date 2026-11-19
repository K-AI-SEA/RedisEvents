using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using RedisEvents.Config;
using RedisEvents.Errors;
using StackExchange.Redis;

namespace RedisEvents.Extensions;

/// <summary>
/// Decides whether two connection strings, or a multiplexer and a connection string, reach the same
/// Redis server. Named connections are compared by <b>endpoint</b>, never by name or by multiplexer
/// identity: three names pointing at one server are one server.
/// </summary>
/// <remarks>
/// <para>
/// An endpoint is <c>host:port</c> with the default port filled in. Two endpoint lists are the same
/// server when they share an endpoint as written, or, failing that, when their host names resolve to
/// a shared address on the same port. The second rule is what makes DNS aliases of one server (a
/// Kubernetes ExternalName per store, all pointing at one Service) count as one server. A name that
/// does not resolve keeps only its written form.
/// </para>
/// <para>
/// Answers are cached for the life of the process. Placement only changes with a restart (moving a
/// store is a config change plus a key move with the writers scaled to zero), so a cached answer
/// cannot go stale while it matters.
/// </para>
/// </remarks>
internal static class RedisServerIdentity
{
    /// <summary>How long one DNS lookup may take before the written name is all that is compared.</summary>
    private static readonly TimeSpan LookupTimeout = TimeSpan.FromSeconds(2);

    private static readonly ConcurrentDictionary<string, string[]> WrittenCache = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, string[]> ResolvedCache = new(StringComparer.Ordinal);
    private static readonly ConditionalWeakTable<IConnectionMultiplexer, ConcurrentDictionary<string, bool>> MultiplexerAnswers = new();

    /// <summary>
    /// Whether two connection strings reach the same server.
    /// </summary>
    /// <param name="left">One connection string.</param>
    /// <param name="right">The other.</param>
    /// <returns><see langword="true"/> when they share an endpoint as written or as resolved.</returns>
    public static bool SameServer(string left, string right)
    {
        if (string.Equals(left, right, StringComparison.Ordinal))
            return true;

        var leftWritten = Written(left);
        var rightWritten = Written(right);

        if (leftWritten.Intersect(rightWritten, StringComparer.OrdinalIgnoreCase).Any())
            return true;

        return Resolved(left).Intersect(Resolved(right), StringComparer.OrdinalIgnoreCase).Any();
    }

    /// <summary>
    /// Whether <paramref name="multiplexer"/> is connected to the server <paramref name="connectionString"/>
    /// names. The answer is cached per multiplexer.
    /// </summary>
    /// <param name="multiplexer">The multiplexer a database came from.</param>
    /// <param name="connectionString">The connection string of the topic being written.</param>
    /// <returns><see langword="true"/> when they share an endpoint as written or as resolved.</returns>
    public static bool SameServer(IConnectionMultiplexer multiplexer, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);

        var answers = MultiplexerAnswers.GetValue(multiplexer, static _ => new ConcurrentDictionary<string, bool>(StringComparer.Ordinal));
        if (answers.TryGetValue(connectionString, out var known))
            return known;

        var answer = Compute(multiplexer, connectionString);
        answers[connectionString] = answer;
        return answer;
    }

    /// <summary>The <c>host:port</c> endpoints of a connection string, as written, default ports filled in.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The endpoints.</returns>
    public static IReadOnlyList<string> Written(string connectionString) =>
        WrittenCache.GetOrAdd(connectionString, static cs => [.. Endpoints(Parse(cs)).Select(Describe)]);

    /// <summary>The default database a connection string selects, or <see langword="null"/> for database 0.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The database index, or <see langword="null"/> when it is not set.</returns>
    public static int? DefaultDatabase(string connectionString) => Parse(connectionString).DefaultDatabase;

    private static bool Compute(IConnectionMultiplexer multiplexer, string connectionString)
    {
        var have = new List<EndPoint>(multiplexer.GetEndPoints());

        // Once connected, GetEndPoints() can report a resolved address where the configuration
        // still carries the DNS name; consult both, as StreamsConnectionProvider.EndpointsMatch does.
        if (!string.IsNullOrWhiteSpace(multiplexer.Configuration))
        {
            try
            {
                have.AddRange(Endpoints(ConfigurationOptions.Parse(multiplexer.Configuration)));
            }
            catch (ArgumentException)
            {
                // An unparseable configuration string contributes nothing.
            }
        }

        var written = have.Select(Describe).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (Written(connectionString).Any(written.Contains))
            return true;

        var resolved = have.SelectMany(ResolveOne).ToHashSet(StringComparer.OrdinalIgnoreCase);
        return Resolved(connectionString).Any(resolved.Contains);
    }

    private static string[] Resolved(string connectionString) =>
        ResolvedCache.GetOrAdd(connectionString, static cs => [.. Endpoints(Parse(cs)).SelectMany(ResolveOne).Distinct(StringComparer.OrdinalIgnoreCase)]);

    private static IEnumerable<string> ResolveOne(EndPoint endpoint)
    {
        yield return Describe(endpoint);

        if (endpoint is not DnsEndPoint dns)
            yield break;

        IPAddress[] addresses;
        try
        {
            var lookup = Dns.GetHostAddressesAsync(dns.Host);
            addresses = lookup.Wait(LookupTimeout) ? lookup.Result : [];
        }
        catch (Exception ex) when (ex is SocketException or AggregateException or ArgumentException)
        {
            addresses = [];
        }

        foreach (var address in addresses)
            yield return $"{(address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address)}:{dns.Port}";
    }

    private static ConfigurationOptions Parse(string connectionString)
    {
        try
        {
            var parsed = ConfigurationOptions.Parse(connectionString);
            parsed.SetDefaultPorts();
            return parsed;
        }
        catch (Exception ex)
        {
            throw new StreamConfigurationException(
                $"{StreamConfigBinder.SectionName}: '{connectionString}' is not a valid Redis connection string: {ex.Message}",
                ex);
        }
    }

    private static IEnumerable<EndPoint> Endpoints(ConfigurationOptions options)
    {
        options.SetDefaultPorts();
        return options.EndPoints;
    }

    private static string Describe(EndPoint endpoint) => endpoint switch
    {
        DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
        IPEndPoint ip => $"{(ip.Address.IsIPv4MappedToIPv6 ? ip.Address.MapToIPv4() : ip.Address)}:{ip.Port}",
        _ => endpoint.ToString() ?? "unknown",
    };
}
