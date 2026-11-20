namespace RedisEvents.Config;

/// <summary>
/// Configuration options for a Redis stream topic.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the properties are <c>set</c> and not <c>init</c> (R-17).</b> Binding goes through the
/// source-generated configuration binder, which assigns members on an already-constructed instance.
/// It cannot assign an <c>init</c>-only property, and — because there is no diagnostic for it — it
/// silently emitted a <c>BindCore</c> that validated the key names and set nothing. Every value under
/// <c>Streams:Topics:*</c> was discarded and every topic ran on record defaults. The records stay
/// records (<c>with</c>-expressions are used throughout the library and the tests); only the
/// accessors changed.
/// </para>
/// </remarks>
public sealed record TopicOptions
{
    /// <summary>The <see cref="MaxLen"/> in force when the key is not configured.</summary>
    public const long DefaultMaxLen = 10_000;

    private long maxLen = DefaultMaxLen;

    /// <summary>
    /// Number of partitions for this topic (default: 2).
    /// </summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// Maximum length of the stream; used for trimming (default: 10000).
    /// </summary>
    /// <remarks>
    /// Assigning this — from configuration or from an object initializer — also records that the
    /// ceiling was chosen rather than defaulted, which is what makes the
    /// <see cref="RetentionSeconds"/> pairing rule in <see cref="StreamConfigBinder"/> reachable.
    /// See <see cref="MaxLenConfigured"/>.
    /// </remarks>
    public long MaxLen
    {
        get => this.maxLen;
        set
        {
            this.maxLen = value;
            this.MaxLenConfigured = true;
        }
    }

    /// <summary>
    /// Whether <see cref="MaxLen"/> was explicitly set rather than defaulted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Decision (R-17), recorded rather than reverted.</b> The remediation note asked for
    /// <c>MaxLen</c> to become <c>long?</c> so that "RetentionSeconds without MaxLen" could fire.
    /// A nullable <c>MaxLen</c> would have changed the meaning of four call sites outside this file
    /// <em>without</em> a compile error — <c>effective.MaxLen &lt; 1</c> is <see langword="false"/>
    /// for <see langword="null"/>, and <c>maxLength: topicOptions.MaxLen</c> would have passed
    /// <see langword="null"/> to <c>XADD</c>, silently dropping the inline <c>MAXLEN</c> that is the
    /// only thing bounding a stream today. Tracking "was it assigned" keeps <c>MaxLen</c> a non-null
    /// <see cref="long"/> with its 10,000 default, so every consumer of it is unchanged, and still
    /// distinguishes configured from defaulted. Internal so it is neither public API nor a bindable
    /// configuration key.
    /// </para>
    /// </remarks>
    internal bool MaxLenConfigured { get; private set; }

    /// <summary>
    /// Trim mode for this topic (default: Approx).
    /// </summary>
    public TrimMode Trim { get; set; } = TrimMode.Approx;

    /// <summary>
    /// Optional retention time in seconds; if set, must be paired with MaxLen.
    /// </summary>
    public int? RetentionSeconds { get; set; }

    /// <summary>
    /// Background trim interval in seconds; 0 = off.
    /// </summary>
    public int BackgroundTrimIntervalSeconds { get; set; }

    /// <summary>
    /// Hash-tag partitions into one slot to co-locate them (default: true).
    /// </summary>
    public bool CoLocatePartitions { get; set; } = true;

    /// <summary>
    /// Whether state-stream entries written by a state store on this topic also keep the event's
    /// correlation id, <c>traceparent</c> and headers (default: <see langword="false"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Off, a state entry is the event body and its type and nothing else — all an aggregate load
    /// reads. The topic copy of every event keeps its full metadata either way; that is the copy
    /// projections and consumers read. The partition key is never written on a state entry: the state
    /// stream's name already identifies the aggregate, and a state stream is never trimmed, so a
    /// per-entry copy would cost its bytes forever.
    /// </para>
    /// <para>
    /// Turn it on to keep a permanent record of which request wrote each event (the topic copy's
    /// trace is trimmed away with the topic). Flipping it is safe at any time: readers treat all of
    /// these fields as optional, so one state stream can hold entries of both shapes.
    /// </para>
    /// </remarks>
    public bool StateMetadata { get; set; }

    /// <summary>
    /// Release the trim clamp above this fraction of MaxLen (default: 0.8).
    /// Used to prevent a stuck consumer from holding up trimming and filling the stream.
    /// </summary>
    public double ClampReleaseThreshold { get; set; } = 0.8;

    /// <summary>
    /// The name of the <see cref="StreamOptions.Connections"/> entry this topic lives on, or
    /// <see langword="null"/> (the default) for <see cref="StreamOptions.ConnectionString"/>.
    /// </summary>
    /// <remarks>
    /// Every key the library keeps for the topic follows it: the partition streams, consumer
    /// positions, ownership, reset markers, topic metadata and the state and dedupe keys of an outbox
    /// or event store. An unknown name fails at startup.
    /// </remarks>
    public string? Connection { get; set; }

    private ConnectionBinding binding = ConnectionBinding.None;

    /// <summary>
    /// The connection string this topic lives on, resolved from <see cref="Connection"/> when read
    /// (and cached) once the topic belongs to a <see cref="StreamOptions"/> that uses
    /// <see cref="StreamOptions.Connections"/>; <see langword="null"/> in single-server mode and for
    /// options built by hand. The outbox compares a database's endpoint against it, so a transaction
    /// can never write a topic's keys onto another server. Internal: not a configuration key.
    /// </summary>
    /// <remarks>
    /// Resolved on use rather than stamped at validation, so a topic added to the options after
    /// startup validation is guarded exactly like a configured one. Setting it pins the value.
    /// </remarks>
    /// <exception cref="Errors.StreamConfigurationException">
    /// <see cref="Connection"/> names a connection the owning options do not configure.
    /// </exception>
    internal string? ResolvedConnectionString
    {
        get => this.binding.Resolve(this.Connection);
        set => this.binding = new ConnectionBinding(this.binding.Owner, value);
    }

    /// <summary>
    /// Records the options this topic belongs to, whose connections <see cref="ResolvedConnectionString"/>
    /// resolves against. Every library path that looks a topic up in <see cref="StreamOptions.Topics"/> calls it.
    /// </summary>
    /// <param name="owner">The options holding this topic.</param>
    internal void BelongsTo(StreamOptions owner)
    {
        if (!ReferenceEquals(this.binding.Owner, owner))
            this.binding = new ConnectionBinding(owner, this.binding.Pinned);
    }

    /// <summary>
    /// Where the topic's connection is resolved from, and the cached answer. Not part of the topic's
    /// value: it compares equal to every other binding, so record equality and hashing see only the
    /// configured members.
    /// </summary>
    private sealed class ConnectionBinding(StreamOptions? owner, string? pinned)
    {
        public static readonly ConnectionBinding None = new(null, null);

        private Resolved? cache;

        public StreamOptions? Owner => owner;

        public string? Pinned => pinned;

        public string? Resolve(string? name)
        {
            if (pinned is not null)
                return pinned;

            if (owner is null || owner.Connections is not { Count: > 0 })
                return null;

            if (this.cache is { } cached && string.Equals(cached.Name, name, StringComparison.Ordinal))
                return cached.ConnectionString;

            var connectionString = string.IsNullOrWhiteSpace(name)
                ? StreamConfigBinder.ResolveConnectionString(owner)
                : StreamConfigBinder.NamedConnectionString(owner, name);
            this.cache = new Resolved(name, connectionString);
            return connectionString;
        }

        public override bool Equals(object? obj) => obj is ConnectionBinding;

        public override int GetHashCode() => 0;

        private sealed record Resolved(string? Name, string ConnectionString);
    }
}
