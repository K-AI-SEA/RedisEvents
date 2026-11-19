using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.EventSourcing;
using RedisEvents.EventSourcing.Sample.Inventory;
using RedisEvents.Producer;
using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>Registers this assembly's two-server collection; see <see cref="TwoRedisCollection"/>.</summary>
[CollectionDefinition(TwoRedisCollection.Name)]
public sealed class TwoRedisCollectionRegistration : ICollectionFixture<TwoRedisFixture>;

/// <summary>
/// An event store on a topic named for another server. The service's home (its default connection)
/// is A; the inventory topic names connection <c>b</c>, on B. Every save appends the aggregate's state
/// stream and publishes its events on B in one transaction there, the projector reads them from B, and
/// the view — the service's own state — is written on A.
/// </summary>
[Collection(TwoRedisCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class NamedConnectionEventStoreTests(TwoRedisFixture fixture)
{
    private const string Consumer = "inventory-svc";

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_event_store_on_a_topic_named_for_B_appends_to_B_and_its_view_lands_on_the_home()
    {
        await fixture.FlushAllAsync();
        var topic = $"named-inventory-{Environment.ProcessId}";

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Streams:ConnectionString"] = fixture.A.ConnectionString,
            ["Streams:Consumer"] = Consumer,
            ["Streams:Connections:b"] = fixture.B.ConnectionString,
            [$"Streams:Topics:{topic}:Connection"] = "b",
        });

        builder.AddEventStore(topic, InventoryEventTypes.Register)
               .AddEventProjector(topic)
               .AddRedisViewStore<InventoryDetail>(topic, "detail", InventoryJsonContext.Default.InventoryDetail)
               .AddProjection<InventoryDetailProjection>();

        using var host = builder.Build();
        await host.StartAsync();

        try
        {
            var repository = host.Services.GetRequiredService<IEventRepository>();
            var views = host.Services.GetRequiredService<IViewStore<InventoryDetail>>();

            var item = InventoryItem.Create("item-1", "Widget");
            item.CheckIn(5);
            await repository.SaveAsync(item);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => await views.GetAsync("item-1") is { CurrentCount: 5 },
                TimeSpan.FromSeconds(15),
                "the projector to read the save from B and write the view on A");

            (await repository.LoadAsync<InventoryItem>("item-1"))!.Version.Should().Be(2, "the aggregate loads both events back from B");
        }
        finally
        {
            await host.StopAsync();
        }

        var onA = await fixture.A.KeysAsync();
        var onB = await fixture.B.KeysAsync();

        onB.Should().Contain(StreamKeys.Stream(topic, 0).ToString(), "the topic lives on B");
        onB.Should().Contain(k => k.Contains(":state:", StringComparison.Ordinal), "the aggregate's state stream is in the topic's transaction, on B");
        onB.Should().Contain(StreamKeys.Positions(topic, Consumer).ToString(), "the projector's position lives beside the stream");
        onA.Should().NotBeEmpty().And.OnlyContain(
            k => !k.StartsWith("re:s:", StringComparison.Ordinal) && !k.StartsWith("re:p:", StringComparison.Ordinal) && !k.Contains(":state:", StringComparison.Ordinal),
            "A, the home, holds the view and nothing of the topic");
    }
}
