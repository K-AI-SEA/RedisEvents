using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;
using RedisEvents.Trimming;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>Registers this assembly's two-server collection; see <see cref="TwoRedisCollection"/>.</summary>
[CollectionDefinition(TwoRedisCollection.Name)]
public sealed class TwoRedisCollectionRegistration : ICollectionFixture<TwoRedisFixture>;

/// <summary>
/// Named connections against two real servers: a topic's keys — streams, positions, ownership,
/// metadata, state and outbox keys — all live on the topic's connection, a service's own keys stay on
/// its default connection, the outbox refuses a database on the wrong server, and pointing every name
/// at one server changes nothing at all.
/// </summary>
/// <remarks>
/// Each of these fails silently without the per-topic routing, which is why it needs two servers: on
/// one server a key written through the wrong connection still lands where the reader looks.
/// </remarks>
[Collection(TwoRedisCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class NamedConnectionTests(TwoRedisFixture fixture)
{
    private const string Service = "named-svc";

    private static int topicCounter;

    /// <summary>
    /// Single-server mode is byte for byte the release before named connections: the same keys while
    /// running and after, and the same number of connections.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task No_connections_section_leaves_the_release_keys_and_connection_count()
    {
        await fixture.FlushAllAsync();

        var (keys, running, clients) = await GoldenScenario.RunAsync(fixture.A);

        running.Should().Equal(GoldenScenario.GoldenRunningKeys, "single-server mode must not rename or add a key");
        keys.Should().Equal(GoldenScenario.GoldenKeys, "single-server mode must not rename or add a key");
        clients.Should().Be(GoldenScenario.GoldenClients, "single-server mode must open exactly the release's connections");
        (await fixture.B.KeysAsync()).Should().BeEmpty();
    }

    /// <summary>
    /// D5a: the full multi-store configuration with every name on one server is the same deployment
    /// as no configuration at all — same keys, same connection count — and the scenario's outbox
    /// write through the default (home) database to the ledger topic, which is named for another
    /// connection, succeeds.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task All_names_on_one_server_leave_the_release_keys_and_connection_count()
    {
        await fixture.FlushAllAsync();

        var (keys, running, clients) = await GoldenScenario.RunAsync(fixture.A, new Dictionary<string, string?>
        {
            ["Streams:Connections:core"] = fixture.A.ConnectionString,
            ["Streams:Connections:feed"] = fixture.A.ConnectionString,
            ["Streams:Connections:bet"] = fixture.A.ConnectionString,
            ["Streams:Topics:{orders}:Connection"] = "feed",
            ["Streams:Topics:{ledger}:Connection"] = "bet",
        });

        running.Should().Equal(GoldenScenario.GoldenRunningKeys, "placement must never change a key name");
        keys.Should().Equal(GoldenScenario.GoldenKeys, "placement must never change a key name");
        clients.Should().Be(GoldenScenario.GoldenClients, "three names on one server must share one multiplexer");
        (await fixture.B.KeysAsync()).Should().BeEmpty();
    }

    /// <summary>
    /// D5a, the alias form: names that spell the same server differently (a host name and an address,
    /// as DNS aliases of one Redis do) still share one multiplexer and leave the release's keys.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Names_spelling_one_server_differently_still_share_its_multiplexer()
    {
        await fixture.FlushAllAsync();

        var port = fixture.A.Port.ToString(CultureInfo.InvariantCulture);

        var (keys, running, clients) = await GoldenScenario.RunAsync(fixture.A, new Dictionary<string, string?>
        {
            ["Streams:Connections:core"] = fixture.A.ConnectionString,
            ["Streams:Connections:feed"] = $"localhost:{port},abortConnect=false",
            ["Streams:Connections:bet"] = $"127.0.0.1:{port},abortConnect=false,connectTimeout=10000",
            ["Streams:Topics:{orders}:Connection"] = "feed",
            ["Streams:Topics:{ledger}:Connection"] = "bet",
        });

        running.Should().Equal(GoldenScenario.GoldenRunningKeys);
        keys.Should().Equal(GoldenScenario.GoldenKeys);
        clients.Should().Be(GoldenScenario.GoldenClients, "names that resolve to one server must share one multiplexer");
    }

    /// <summary>
    /// A consumer whose default connection is B reads a topic that lives on A, in every read mode, and
    /// writes its views on B. Everything the library keeps for the topic is on A; nothing of it is on B.
    /// </summary>
    [Theory]
    [Trait("TestType", "ServiceTest")]
    [InlineData(ReadMode.Block, DeliveryMode.Ordered)]
    [InlineData(ReadMode.Poll, DeliveryMode.Ordered)]
    [InlineData(ReadMode.Poll, DeliveryMode.WorkQueue)]
    public async Task Consumes_a_topic_on_A_while_its_views_are_written_on_B(ReadMode readMode, DeliveryMode delivery)
    {
        await fixture.FlushAllAsync();
        var topic = NewTopic();

        var builder = Builder(fixture.B, new Dictionary<string, string?>
        {
            ["Streams:Connections:a"] = fixture.A.ConnectionString,
            [$"Streams:Topics:{topic}:Connection"] = "a",
            [$"Streams:Topics:{topic}:Partitions"] = "2",
            ["Streams:Consumers:0:Topic"] = topic,
            ["Streams:Consumers:0:ReadMode"] = readMode.ToString(),
            ["Streams:Consumers:0:Delivery"] = delivery.ToString(),
            ["Streams:Consumers:0:BlockMs"] = "200",
            ["Streams:Consumers:0:PersistIntervalMs"] = "100",
        });

        IServiceProvider? services = null;
        var seen = 0;

        builder.AddStreamPublisher(topic);
        builder.AddStream(topic, async (batch, _) =>
        {
            // A view is the service's own state, so it goes on the default connection: B.
            var views = StreamsConnection.GetSharedDatabase(services!);
            foreach (var message in batch.ToArray())
                await views.StringSetAsync($"view:{Encoding.UTF8.GetString(message.BodySpan)}", "seen");

            Interlocked.Add(ref seen, batch.Length);
        });

        using var host = builder.Build();
        services = host.Services;
        await host.StartAsync();

        try
        {
            var publisher = host.Services.GetRequiredService<IStreamPublisher>();
            for (var i = 0; i < 6; i++)
                await publisher.PublishAsync($"key-{i}", Encoding.UTF8.GetBytes($"m{i}"), "named.test");

            await RedisStreamsFixture.WaitUntilAsync(() => Volatile.Read(ref seen) >= 6, TimeSpan.FromSeconds(30), "every message on A to be consumed");

            // The process-wide lag sampler, started on B, still samples this topic's stream on A.
            var monitors = StreamLag.All.Where(m => m.Topic == topic).ToArray();
            monitors.Should().NotBeEmpty().And.OnlyContain(m => m.Connection != null);
            await new StreamLagSampler(fixture.B.Admin).SampleOnceAsync(CancellationToken.None);
            (await Task.WhenAll(monitors.Select(m => fixture.A.Db.StreamLengthAsync(m.StreamKey)))).Sum().Should().Be(6);

            foreach (var monitor in monitors)
            {
                var last = await fixture.A.Db.StreamRangeAsync(monitor.StreamKey, "-", "+", 1, Order.Descending);
                var expected = last.Length == 0
                    ? StreamId.Min
                    : StreamId.TryParse(last[0].Id.ToString(), out var id) ? id : StreamId.Min;

                monitor.ReadProgress().Tail.Should().Be(expected, "the tail is sampled on A, where the partition lives, not on the sampler's B");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300));
        }
        finally
        {
            await host.StopAsync();
        }

        var onA = await fixture.A.KeysAsync();
        var onB = await fixture.B.KeysAsync();

        onB.Should().BeEquivalentTo(Enumerable.Range(0, 6).Select(i => $"view:m{i}"), "B holds the service's views and nothing of the topic");
        onA.Should().OnlyContain(k => k.Contains(topic, StringComparison.Ordinal), "A holds the topic's keys and nothing else");
        onA.Should().Contain(StreamKeys.Stream(topic, 0).ToString()).And.Contain(StreamKeys.Stream(topic, 1).ToString());
        onA.Should().Contain(k => k.StartsWith("re:m:", StringComparison.Ordinal), "topic metadata lives with the topic");

        if (delivery == DeliveryMode.Ordered)
            onA.Should().Contain(StreamKeys.Positions(topic, Service).ToString(), "positions live beside the stream they track");
    }

    /// <summary>
    /// The trimmer on A clamps to the position of a consumer whose default connection is B, because
    /// that position was written on A, beside the stream. Were it written on B, the clamp scan on A
    /// would find no consumer and trim every entry, including the five it has not read.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_trimmer_on_A_clamps_to_a_consumer_whose_default_is_B()
    {
        await fixture.FlushAllAsync();
        var topic = NewTopic();

        var builder = Builder(fixture.B, new Dictionary<string, string?>
        {
            ["Streams:Connections:a"] = fixture.A.ConnectionString,
            [$"Streams:Topics:{topic}:Connection"] = "a",
            [$"Streams:Topics:{topic}:Partitions"] = "1",
            [$"Streams:Topics:{topic}:MaxLen"] = "100000",
            [$"Streams:Topics:{topic}:Trim"] = "Exact",
            [$"Streams:Topics:{topic}:RetentionSeconds"] = "1",
            ["Streams:Consumers:0:Topic"] = topic,
            ["Streams:Consumers:0:ReadMode"] = "Poll",
            ["Streams:Consumers:0:PersistIntervalMs"] = "100",
        });

        var seen = 0;
        builder.AddStreamPublisher(topic);
        builder.AddStream(topic, (batch, _) =>
        {
            Interlocked.Add(ref seen, batch.Length);
            return ValueTask.CompletedTask;
        });

        using var host = builder.Build();
        var options = host.Services.GetRequiredService<StreamOptions>();
        await host.StartAsync();

        try
        {
            var publisher = host.Services.GetRequiredService<IStreamPublisher>();
            for (var i = 0; i < 5; i++)
                await publisher.PublishAsync("k", Encoding.UTF8.GetBytes($"read-{i}"), "named.test");

            await RedisStreamsFixture.WaitUntilAsync(() => Volatile.Read(ref seen) >= 5, TimeSpan.FromSeconds(30), "the first five to be consumed");
        }
        finally
        {
            // Stopping flushes the position, at the fifth entry.
            await host.StopAsync();
        }

        (await fixture.A.Db.KeyExistsAsync(StreamKeys.Positions(topic, Service))).Should().BeTrue("the position is written on the topic's server");
        (await fixture.B.KeysAsync()).Should().BeEmpty("nothing of the topic belongs on the consumer's default server");

        // Five more the consumer has not read, then let all ten age past the one-second retention.
        var direct = new StreamPublisher(fixture.A.Db, topic, options.Topics[topic]);
        for (var i = 0; i < 5; i++)
            await direct.PublishAsync("k", Encoding.UTF8.GetBytes($"unread-{i}"), "named.test");

        await Task.Delay(TimeSpan.FromMilliseconds(1500));

        var trimmer = new BackgroundTrimmer(options, host.Services.GetRequiredService<StreamsConnectionRegistry>());
        _ = await trimmer.TrimTopicOnceAsync(topic);

        // MINID keeps the entry at the clamp itself, so what survives is the consumer's last read
        // entry and the five it has not read; a trim that saw no consumer would have left nothing.
        (await fixture.A.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(
            6,
            "the clamp holds the consumer's stored position and every entry after it, however old");
    }

    /// <summary>An outbox write to a topic on B, with its state on B, through B's database: one transaction on B.</summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Outbox_to_a_topic_on_B_with_state_on_B_succeeds()
    {
        await fixture.FlushAllAsync();
        var topic = NewTopic();
        using var host = HomeOnAWithTopicOnB(topic);
        var options = host.Services.GetRequiredService<StreamOptions>();
        var state = Outbox.StateKey(topic, "balance:1");

        var onB = StreamsConnection.GetTopicDatabase(host.Services, topic);

        var id = await Outbox.WriteAndPublishAsync(
            onB,
            tran => _ = tran.StringSetAsync(state, "10"),
            topic,
            "1",
            Encoding.UTF8.GetBytes("credited"),
            "named.credited",
            stateKeys: [state],
            topicOptions: options.Topics[topic]);

        id.Should().NotBeNull();

        var many = await Outbox.WriteAndPublishManyAsync(
            onB,
            tran => _ = tran.StringSetAsync(state, "20"),
            new[] { new OutboxPublish(topic, "1", Encoding.UTF8.GetBytes("again"), "named.credited", TopicOptions: options.Topics[topic]) },
            stateKeys: [state]);

        many.Should().NotBeNull();

        var forwarder = new TopicForwarder(onB, topic, options);
        (await forwarder.ForwardAsync("source", "dedupe-1", new StreamId(1, 0), [new ForwardedMessage("1", Encoding.UTF8.GetBytes("fwd"), "named.fwd")]))
            .Should().BeTrue();

        (await fixture.B.Db.StringGetAsync(state)).ToString().Should().Be("20");
        (await fixture.B.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(3);
        (await fixture.A.KeysAsync()).Should().BeEmpty("nothing of a topic on B belongs on A");
    }

    /// <summary>
    /// An outbox write to a topic on B through A's database — the service's home — throws before
    /// anything is sent: queued there, the state and the stream entry would land on A, where no
    /// consumer or event store of the topic ever looks.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Outbox_to_a_topic_on_B_through_As_database_throws()
    {
        await fixture.FlushAllAsync();
        var topic = NewTopic();
        using var host = HomeOnAWithTopicOnB(topic);
        var options = host.Services.GetRequiredService<StreamOptions>();
        var state = Outbox.StateKey(topic, "balance:1");

        var home = StreamsConnection.GetSharedDatabase(host.Services);

        var single = () => Outbox.WriteAndPublishAsync(
            home,
            tran => _ = tran.StringSetAsync(state, "10"),
            topic,
            "1",
            Encoding.UTF8.GetBytes("credited"),
            "named.credited",
            topicOptions: options.Topics[topic]);

        (await single.Should().ThrowAsync<StreamConfigurationException>())
            .WithMessage($"*'{topic}'*connection 'b'*");

        var many = () => Outbox.WriteAndPublishManyAsync(
            home,
            tran => _ = tran.StringSetAsync(state, "10"),
            new[] { new OutboxPublish(topic, "1", Encoding.UTF8.GetBytes("credited"), "named.credited", TopicOptions: options.Topics[topic]) });

        await many.Should().ThrowAsync<StreamConfigurationException>();

        var forwarder = () => new TopicForwarder(home, topic, options);
        forwarder.Should().Throw<StreamConfigurationException>("a forwarder onto a topic on B must not be built over A");

        (await fixture.A.KeysAsync()).Should().BeEmpty("the refused transaction must not have written anything");
        (await fixture.B.KeysAsync()).Should().BeEmpty();
    }

    /// <summary>
    /// The state store behind an event store, on a topic named for B in a service whose home is A,
    /// appends the state stream and publishes the event on B, in one transaction there.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_stream_store_on_a_topic_named_for_B_appends_to_B()
    {
        await fixture.FlushAllAsync();
        var topic = NewTopic();
        using var host = HomeOnAWithTopicOnB(topic);

        var store = host.Services.GetRequiredService<IStreamStore>();
        var ids = await store.AppendAndPublishAsync(
            "es:Account:1",
            expectedLength: 0,
            "1",
            [new StateEvent(Encoding.UTF8.GetBytes("opened"), "named.opened")]);

        ids.Should().NotBeNull();
        (await store.ReadAsync("es:Account:1", StreamId.Min, 10)).Should().ContainSingle();
        (await fixture.B.Db.StreamLengthAsync(Outbox.StateKey(topic, "es:Account:1"))).Should().Be(1);
        (await fixture.B.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(1);
        (await fixture.A.KeysAsync()).Should().BeEmpty();
    }

    private static string NewTopic() => $"named{Interlocked.Increment(ref topicCounter)}x{Environment.ProcessId}";

    private static HostApplicationBuilder Builder(TwoRedisFixture.Server home, IDictionary<string, string?> config)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(config)
        {
            ["Streams:ConnectionString"] = home.ConnectionString,
            ["Streams:Consumer"] = Service,
        });

        return builder;
    }

    /// <summary>A built, unstarted host whose home is A, with one topic named for connection 'b' on B and a store on it.</summary>
    private IHost HomeOnAWithTopicOnB(string topic)
    {
        var builder = Builder(fixture.A, new Dictionary<string, string?>
        {
            ["Streams:Connections:b"] = fixture.B.ConnectionString,
            [$"Streams:Topics:{topic}:Connection"] = "b",
            [$"Streams:Topics:{topic}:Partitions"] = "1",
        });

        builder.AddStreamStore(topic);
        return builder.Build();
    }
}
