using System;
using System.Text;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using pefi.Rabbit;


string InfluxUrl = "http://localhost:8086";
string InfluxToken = "my-super-secret-token";
string InfluxOrg = "myorg";
string InfluxBucket = "mybucket";


var messageBroker = new MessageBroker("192.168.0.5", "username", "password");
var influxClient = InfluxDBClientFactory.Create(InfluxUrl, InfluxToken.ToCharArray());

var writeApi = influxClient.GetWriteApi();

using var topic = await messageBroker.CreateTopic("Events");

await topic.Subscribe("#", async (key, message)
    => await doasdf(message));

Console.WriteLine("Waiting for messages... Press [Enter] to exit.");
Console.ReadLine();

influxClient.Dispose();


async Task doasdf(string message)
{
    var jObj = JObject.Parse(message);

    var timestamp = jObj["timestamp"]?.ToObject<DateTime>() ?? DateTime.UtcNow;
    var point = PointData.Measurement("dynamic_data").Timestamp(timestamp, WritePrecision.Ns);

    foreach (var prop in jObj.Properties())
    {
        if (prop.Name == "timestamp") continue;

        var name = prop.Name;
        var value = prop.Value;

        switch (value.Type)
        {
            case JTokenType.String:
                point = point.Tag(name, value.ToString());
                break;
            case JTokenType.Float:
            case JTokenType.Integer:
                point = point.Field(name, value.ToObject<double>());
                break;
            case JTokenType.Boolean:
                point = point.Field(name, value.ToObject<bool>());
                break;
            default:
                Console.WriteLine($"[!] Skipping unsupported type for '{name}'");
                break;
        }
    }

    writeApi.WritePoint(point, InfluxBucket, InfluxOrg);
    Console.WriteLine("[✓] Data written to InfluxDB");
    await Task.CompletedTask;
}
