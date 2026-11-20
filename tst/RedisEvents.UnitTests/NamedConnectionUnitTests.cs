using System.Net;
using System.Reflection;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// Named connections without a server: the configuration rules, the grouping of names by server, the
/// outbox's endpoint guard, and the health check's view of every connection in use. The two-server
/// behaviour is proved in <c>RedisEvents.Tests.NamedConnectionTests</c>.
/// </summary>
public class NamedConnectionUnitTests
{
    /// <summary>A closed port; nothing here ever connects to it successfully.</summary>
    private const string Unreachable = "127.0.0.1:6399,connectTimeout=200,connectRetry=0,abortConnect=false";

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_naming_an_unknown_connection_fails_validation_naming_the_key()
    {
        var options = new StreamOptions
        {
            Connections = { ["feed"] = "redis-db-feed:6379" },
            Topics = { ["orders"] = new TopicOptions { Connection = "bet" } },
        };

        var validate = () => StreamConfigBinder.Validate(options);

        validate.Should().Throw<StreamConfigurationException>()
            .WithMessage("*Streams:Topics:orders:Connection*'bet'*feed*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_naming_a_connection_with_no_connections_section_fails_validation()
    {
        var options = new StreamOptions { Topics = { ["orders"] = new TopicOptions { Connection = "feed" } } };

        var validate = () => StreamConfigBinder.Validate(options);

        validate.Should().Throw<StreamConfigurationException>().WithMessage("*Streams:Connections is empty*");
    }

    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData("")]
    [InlineData("   ")]
    public void An_empty_named_connection_string_fails_validation(string value)
    {
        var options = new StreamOptions { Connections = { ["feed"] = value } };

        var validate = () => StreamConfigBinder.Validate(options);

        validate.Should().Throw<StreamConfigurationException>().WithMessage("*Streams:Connections:feed*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Connection_names_are_matched_case_insensitively_like_configuration_keys()
    {
        var options = new StreamOptions
        {
            Connections = { ["Feed"] = "redis-db-feed:6379" },
            Topics = { ["orders"] = new TopicOptions { Connection = "feed" } },
        };

        StreamConfigBinder.Validate(options);

        StreamConfigBinder.ResolveConnectionString(options, "orders").Should().Be("redis-db-feed:6379");
    }

    /// <summary>
    /// Every configured topic resolves to its connection string once named connections are in use —
    /// the default one for a topic that names none — and to nothing without them.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Each_topic_resolves_to_its_connection_only_when_connections_are_in_use()
    {
        var named = new StreamOptions
        {
            ConnectionString = "redis-db:6379",
            Connections = { ["feed"] = "redis-db-feed:6379" },
            Topics = { ["orders"] = new TopicOptions { Connection = "feed" }, ["audit"] = new TopicOptions() },
        };
        var single = new StreamOptions { Topics = { ["orders"] = new TopicOptions() } };

        StreamConfigBinder.Validate(named);
        StreamConfigBinder.Validate(single);

        named.Topics["orders"].ResolvedConnectionString.Should().Be("redis-db-feed:6379");
        named.Topics["audit"].ResolvedConnectionString.Should().Be("redis-db:6379");
        single.Topics["orders"].ResolvedConnectionString.Should().BeNull("single-server mode must carry nothing new");
    }

    /// <summary>A Block consumer of a topic on a named connection is checked against that connection's syncTimeout.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_block_consumer_is_checked_against_its_topics_connection_timeout()
    {
        var options = new StreamOptions
        {
            ConnectionString = "redis-db:6379,syncTimeout=10000",
            Connections = { ["feed"] = "redis-db-feed:6379,syncTimeout=1000" },
            Topics = { ["orders"] = new TopicOptions { Connection = "feed" } },
            Consumers = [new ConsumerOptions { Topic = "orders", ReadMode = ReadMode.Block, BlockMs = 2000 }],
        };

        var validate = () => StreamConfigBinder.Validate(options);

        validate.Should().Throw<StreamConfigurationException>().WithMessage("*syncTimeout is 1000ms*Streams:Connections:feed*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void With_no_connections_every_topic_resolves_to_the_default_provider()
    {
        var options = new StreamOptions { ConnectionString = "redis-db:6379", Topics = { ["orders"] = new TopicOptions() } };
        using var registry = Registry(options);

        registry.Distinct.Should().ContainSingle().Which.Should().BeSameAs(registry.Default);
        registry.ForTopic("orders").Should().BeSameAs(registry.Default);
        registry.ForTopic("never-configured").Should().BeSameAs(registry.Default);
    }

    /// <summary>D5a: three names on the default connection's server are one provider, and so one multiplexer.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Names_on_the_default_server_share_the_default_provider()
    {
        var options = new StreamOptions
        {
            ConnectionString = "redis-db:6379,abortConnect=false",
            Connections =
            {
                ["core"] = "redis-db:6379,abortConnect=false",
                ["feed"] = "redis-db:6379,allowAdmin=true",
                ["bet"] = "REDIS-DB:6379",
            },
            Topics = { ["orders"] = new TopicOptions { Connection = "feed" }, ["bets"] = new TopicOptions { Connection = "bet" } },
        };
        using var registry = Registry(options);

        registry.Distinct.Should().ContainSingle();
        registry.ForName("core").Should().BeSameAs(registry.Default);
        registry.ForTopic("orders").Should().BeSameAs(registry.Default);
        registry.ForTopic("bets").Should().BeSameAs(registry.Default);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Names_on_another_server_get_one_provider_per_server()
    {
        var options = new StreamOptions
        {
            ConnectionString = "10.0.0.1:6379",
            Connections =
            {
                ["core"] = "10.0.0.1:6379",
                ["feed"] = "10.0.0.2:6379",
                ["feed-again"] = "10.0.0.2:6379,abortConnect=false",
                ["bet"] = "10.0.0.3:6379",
            },
        };
        using var registry = Registry(options);

        registry.Distinct.Should().HaveCount(3);
        registry.ForName("core").Should().BeSameAs(registry.Default);
        registry.ForName("feed").Should().BeSameAs(registry.ForName("feed-again"));
        registry.ForName("feed").Should().NotBeSameAs(registry.Default);
        registry.ForName("bet").Should().NotBeSameAs(registry.ForName("feed"));
        registry.ForName("feed").ConnectionString.Should().StartWith("10.0.0.2:6379");
    }

    /// <summary>Two spellings of one server — a host name and the address it resolves to — are one server.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_host_name_and_its_address_are_one_server()
    {
        RedisServerIdentity.SameServer("localhost:6379", "127.0.0.1:6379").Should().BeTrue();
        RedisServerIdentity.SameServer("localhost", "127.0.0.1:6379").Should().BeTrue("the default port is filled in");
        RedisServerIdentity.SameServer("localhost:6379", "127.0.0.1:6380").Should().BeFalse("a different port is a different server");
        RedisServerIdentity.SameServer("10.0.0.1:6379", "10.0.0.2:6379").Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Two_names_on_one_server_selecting_different_databases_are_refused()
    {
        var options = new StreamOptions
        {
            ConnectionString = "10.0.0.1:6379",
            Connections = { ["feed"] = "10.0.0.1:6379,defaultDatabase=2" },
        };

        var build = () => Registry(options);

        build.Should().Throw<StreamConfigurationException>().WithMessage("*Streams:Connections:feed*database 2*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unknown_connection_name_is_refused_by_the_registry()
    {
        using var registry = Registry(new StreamOptions { Connections = { ["feed"] = "10.0.0.2:6379" } });

        var resolve = () => registry.ForName("bet");

        resolve.Should().Throw<StreamConfigurationException>().WithMessage("*'bet'*feed*");
    }

    /// <summary>
    /// The outbox guard compares endpoints: a database on the topic's server passes whatever
    /// multiplexer it came from, one on another server throws, and options with no resolved
    /// connection (single-server mode) are not checked at all.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_outbox_guard_compares_the_databases_endpoint_with_the_topics_connection()
    {
        var onTopicServer = FakeDatabase.At("10.0.0.2:6379");
        var spelledDifferently = FakeDatabase.At("10.0.0.2:6379", configuration: "10.0.0.2:6379,name=elsewhere");
        var onOtherServer = FakeDatabase.At("10.0.0.1:6379");

        var topic = new TopicOptions { Connection = "feed", ResolvedConnectionString = "10.0.0.2:6379,abortConnect=false" };
        var unresolved = new TopicOptions();

        Outbox.RequireTopicServer(onTopicServer, "orders", topic);
        Outbox.RequireTopicServer(spelledDifferently, "orders", topic);
        Outbox.RequireTopicServer(onOtherServer, "orders", unresolved);

        var wrong = () => Outbox.RequireTopicServer(onOtherServer, "orders", topic);
        wrong.Should().Throw<StreamConfigurationException>().WithMessage("*'orders' lives on 10.0.0.2:6379 (connection 'feed')*10.0.0.1:6379*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_outbox_refuses_a_database_on_another_server_before_building_a_transaction()
    {
        var topic = new TopicOptions { Connection = "feed", ResolvedConnectionString = "10.0.0.2:6379" };

        var write = () => Outbox.WriteAndPublishAsync(
            FakeDatabase.At("10.0.0.1:6379"),
            _ => { },
            "orders",
            "k",
            new byte[] { 1 },
            "t",
            topicOptions: topic);

        await write.Should().ThrowAsync<StreamConfigurationException>();
    }

    /// <summary>
    /// The builder registers the registry, and a topic's consumer and publisher resolve the topic's
    /// provider — after which the health check covers that connection too, and reports it down when
    /// it is.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_health_check_covers_every_connection_a_topic_was_resolved_to()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Streams:ConnectionString"] = Unreachable,
            ["Streams:Connections:dead"] = "127.0.0.1:6398,connectTimeout=200,connectRetry=0,abortConnect=false",
            ["Streams:Topics:orders:Connection"] = "dead",
        });
        builder.AddStreamPublisher("orders");
        builder.AddStream("orders", (_, _) => ValueTask.CompletedTask);

        using var host = builder.Build();
        var registry = host.Services.GetRequiredService<StreamsConnectionRegistry>();

        registry.Distinct.Should().HaveCount(2);
        StreamHealth.Connections(host.Services).Should().ContainSingle("only the default is in use before any topic is resolved");

        // Building the consumer host (not starting it) and the publisher resolves the topic's connection.
        _ = host.Services.GetServices<IHostedService>().ToArray();
        _ = host.Services.GetRequiredService<IStreamPublisher>();

        var inUse = StreamHealth.Connections(host.Services);
        inUse.Should().HaveCount(2).And.Contain(registry.ForName("dead"));

        var report = StreamHealth.Evaluate(host.Services);
        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Description.Should().Contain("6398", "the named connection's endpoint is reported");
    }

    /// <summary>
    /// A topic written into the bound options after startup validation — as an application that
    /// names topics in code does — is routed to its connection and guarded like a configured one,
    /// with nothing stamped by hand.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_added_after_validation_is_routed_and_guarded()
    {
        var options = new StreamOptions
        {
            ConnectionString = "10.0.0.1:6379",
            Connections = { ["feed"] = "10.0.0.2:6379,abortConnect=false" },
        };
        using var registry = Registry(options);

        options.Topics["orders"] = new TopicOptions { Connection = "feed" };

        registry.ForTopic("orders").Should().BeSameAs(registry.ForName("feed"));
        var topic = options.Topics["orders"];
        topic.ResolvedConnectionString.Should().Be("10.0.0.2:6379,abortConnect=false");

        Outbox.RequireTopicServer(FakeDatabase.At("10.0.0.2:6379"), "orders", topic);
        var wrong = () => Outbox.RequireTopicServer(FakeDatabase.At("10.0.0.1:6379"), "orders", topic);
        wrong.Should().Throw<StreamConfigurationException>().WithMessage("*'orders' lives on 10.0.0.2:6379 (connection 'feed')*10.0.0.1:6379*");

        var forward = () => new TopicForwarder(FakeDatabase.At("10.0.0.1:6379"), "orders", options);
        forward.Should().Throw<StreamConfigurationException>("the forwarder looks the topic up itself");
    }

    /// <summary>
    /// The same through a host: a topic added to the bound options between builder calls, after the
    /// first one validated them, is on its connection once the container is built — read straight out
    /// of the options, as a service's own outbox call does — and its store is registered on it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_topic_added_to_the_bound_options_between_builder_calls_is_guarded_in_the_built_host()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Streams:ConnectionString"] = "10.0.0.1:6379,abortConnect=false",
            ["Streams:Connections:feed"] = "10.0.0.2:6379,abortConnect=false",
        });
        builder.AddStreamPublisher("audit");
        var bound = (StreamOptions)builder.Services.Single(d => d.ServiceType == typeof(StreamOptions)).ImplementationInstance!;
        bound.Topics["orders"] = new TopicOptions { Connection = "feed" };
        builder.AddStreamStore("orders");

        using var host = builder.Build();
        var registry = host.Services.GetRequiredService<StreamsConnectionRegistry>();
        var topic = host.Services.GetRequiredService<StreamOptions>().Topics["orders"];

        topic.ResolvedConnectionString.Should().Be("10.0.0.2:6379,abortConnect=false");
        registry.ForTopic("orders").Should().BeSameAs(registry.ForName("feed"));

        var write = () => Outbox.WriteAndPublishAsync(
            FakeDatabase.At("10.0.0.1:6379"),
            _ => { },
            "orders",
            "k",
            new byte[] { 1 },
            "t",
            topicOptions: topic);
        await write.Should().ThrowAsync<StreamConfigurationException>().WithMessage("*'orders' lives on 10.0.0.2:6379*");
    }

    /// <summary>A topic added after validation that names an unknown connection fails where it is used, naming both.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_added_after_validation_naming_an_unknown_connection_fails_on_use()
    {
        var options = new StreamOptions
        {
            ConnectionString = "10.0.0.1:6379",
            Connections = { ["feed"] = "10.0.0.2:6379" },
        };
        using var registry = Registry(options);

        options.Topics["orders"] = new TopicOptions { Connection = "bet" };

        var route = () => registry.ForTopic("orders");
        route.Should().Throw<StreamConfigurationException>().WithMessage("*topic 'orders'*'bet'*feed*");

        var guard = () => Outbox.RequireTopicServer(FakeDatabase.At("10.0.0.2:6379"), "orders", options.Topics["orders"]);
        guard.Should().Throw<StreamConfigurationException>().WithMessage("*topic 'orders'*'bet'*feed*");
    }

    /// <summary>Without named connections a topic added later resolves to nothing and is not guarded, as before.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_added_after_validation_in_single_server_mode_resolves_to_nothing()
    {
        var options = new StreamOptions { ConnectionString = "10.0.0.1:6379" };
        using var registry = Registry(options);

        options.Topics["orders"] = new TopicOptions();

        registry.ForTopic("orders").Should().BeSameAs(registry.Default);
        options.Topics["orders"].ResolvedConnectionString.Should().BeNull();
        Outbox.RequireTopicServer(FakeDatabase.At("10.0.0.9:6379"), "orders", options.Topics["orders"]);
    }

    /// <summary>Resolving a topic's connection is not part of its value: record equality is unchanged.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_resolved_topic_still_equals_the_same_options_unresolved()
    {
        var options = new StreamOptions
        {
            Connections = { ["feed"] = "10.0.0.2:6379" },
            Topics = { ["orders"] = new TopicOptions { Connection = "feed", Partitions = 4 } },
        };
        StreamConfigBinder.Validate(options);
        _ = options.Topics["orders"].ResolvedConnectionString;

        var plain = new TopicOptions { Connection = "feed", Partitions = 4 };

        options.Topics["orders"].Should().Be(plain);
        options.Topics["orders"].GetHashCode().Should().Be(plain.GetHashCode());
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void GetTopicDatabase_needs_the_streams_registration()
    {
        using var empty = new ServiceCollection().BuildServiceProvider();

        var get = () => StreamsConnection.GetTopicDatabase(empty, "orders");

        get.Should().Throw<StreamConfigurationException>().WithMessage("*'orders'*AddStream*");
    }

    private static StreamsConnectionRegistry Registry(StreamOptions options)
    {
        StreamConfigBinder.Validate(options);
        return new StreamsConnectionRegistry(options, new StreamsConnectionProvider(options, null, null));
    }

    /// <summary>
    /// An <see cref="IDatabase"/> stand-in whose <see cref="IRedisAsync.Multiplexer"/> reports one
    /// endpoint; everything else throws. Copied in shape from <c>ConnectionResolutionTests</c>.
    /// </summary>
    private sealed class FakeDatabase
    {
        private readonly IConnectionMultiplexer multiplexer;

        private FakeDatabase(IConnectionMultiplexer multiplexer) => this.multiplexer = multiplexer;

        internal static IDatabase At(string endpoint, string? configuration = null)
        {
            var proxy = DispatchProxy.Create<IDatabase, DatabaseProxy>();
            ((DatabaseProxy)proxy).Owner = new FakeDatabase(FakeMultiplexer.At(endpoint, configuration));
            return proxy;
        }

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class DatabaseProxy : DispatchProxy
        {
            internal FakeDatabase Owner { get; set; } = null!;

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return targetMethod.Name switch
                {
                    "get_Multiplexer" => this.Owner.multiplexer,
                    "get_Database" => 0,
                    _ => throw new NotSupportedException($"FakeDatabase does not implement {targetMethod.Name}."),
                };
            }
        }
    }

    /// <summary>An <see cref="IConnectionMultiplexer"/> stand-in reporting one endpoint, a configuration and a client name.</summary>
    private sealed class FakeMultiplexer
    {
        private readonly EndPoint[] endpoints;
        private readonly string configuration;

        private FakeMultiplexer(EndPoint[] endpoints, string configuration)
        {
            this.endpoints = endpoints;
            this.configuration = configuration;
        }

        internal static IConnectionMultiplexer At(string endpoint, string? configuration = null)
        {
            var proxy = DispatchProxy.Create<IConnectionMultiplexer, Proxy>();
            ((Proxy)proxy).Owner = new FakeMultiplexer(ConfigurationOptions.Parse(endpoint).EndPoints.ToArray(), configuration ?? endpoint);
            return proxy;
        }

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class Proxy : DispatchProxy
        {
            internal FakeMultiplexer Owner { get; set; } = null!;

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return targetMethod.Name switch
                {
                    "GetEndPoints" => this.Owner.endpoints,
                    "get_Configuration" => this.Owner.configuration,
                    "get_ClientName" => "an-app-connection",
                    _ => throw new NotSupportedException($"FakeMultiplexer does not implement {targetMethod.Name}."),
                };
            }
        }
    }
}
