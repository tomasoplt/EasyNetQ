using EasyNetQ;
using EasyNetQ.Topology;

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, _) => cts.Cancel();

using var bus = RabbitHutch
    .CreateBus("host=localhost:5673;publisherConfirms=True",x => x.EnableNewtonsoftJson()
        .EnableAlwaysNackWithoutRequeueConsumerErrorStrategy()
        .EnableConsoleLogger()
);

var eventQueue = await bus.Advanced
    .QueueDeclareAsync("q.woc.fan", c => c.WithQueueType(QueueType.Classic), cts.Token)
    .ConfigureAwait(false); ;

var fanOutExchange = await bus.Advanced
    .ExchangeDeclareAsync("ex.woc.fan", ExchangeType.Fanout)
    .ConfigureAwait(false);

var exChangeBinding = await bus.Advanced
    .BindAsync(fanOutExchange, eventQueue, string.Empty)
    .ConfigureAwait(false);

await bus.Advanced
    .PublishAsync(fanOutExchange, string.Empty, true, new Message<string>("hello"))
    .ConfigureAwait(false); ;

using var eventsConsumer = bus.Advanced.Consume(eventQueue, (_, _, _) => { });

while (!cts.IsCancellationRequested)
{
    try
    {
        await bus.Advanced.PublishAsync(
            Exchange.Default,
            "Events",
            true,
            new MessageProperties(),
            ReadOnlyMemory<byte>.Empty,
            cts.Token
        );
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception)
    {
        await Task.Delay(5000, cts.Token);
    }
}
