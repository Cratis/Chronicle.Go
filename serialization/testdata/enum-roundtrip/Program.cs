// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Cratis.Chronicle.Events;
using Cratis.Serialization;

if (args.Length != 1) throw new ArgumentException("Usage: EnumRoundtrip.dll INPUT.json");
if (Environment.Version.ToString() != "10.0.12") throw new InvalidOperationException("Requires runtime 10.0.12 exactly");
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
var types = new Dictionary<string, Type>();
foreach (var member in new[] { typeof(Plain), typeof(NoZero), typeof(Bits), typeof(Int32Sample), typeof(AllBits) })
{
    types.Add($"Scalar<{member.Name}>", typeof(Scalar<>).MakeGenericType(member));
    types.Add($"NullableScalar<{member.Name}>", typeof(NullableScalar<>).MakeGenericType(member));
    types.Add($"ArrayValue<{member.Name}>", typeof(ArrayValue<>).MakeGenericType(member));
}
var serializers = new Dictionary<string, EventSerializer>();
foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
    serializers.Add(naming.GetType().FullName!, Isolation.Serializer(Isolation.ClientOptions(naming), new EmptyDerived()));
var cases = JsonNode.Parse(File.ReadAllText(args[0]))!.AsArray();
if (cases.Count == 0) throw new InvalidOperationException("No roundtrip cases");
var count = 0;
foreach (var item in cases)
{
    var test = item!.AsObject();
    var serializer = serializers[test["namingPolicy"]!.GetValue<string>()];
    var type = types[test["declaredType"]!.GetValue<string>()];
    var value = await serializer.Deserialize(type, JsonNode.Parse(test["payload"]!.GetValue<string>())!.AsObject());
    var actual = Snapshot(value);
    if (!JsonNode.DeepEquals(test["expected"], actual))
        throw new InvalidOperationException("Typed roundtrip mismatch at case " + count);
    // Also execute actual reserialization; a typed read whose write fails is not a roundtrip.
    var serialized = await serializer.Serialize(value);
    var reread = await serializer.Deserialize(type, serialized);
    if (!JsonNode.DeepEquals(actual, Snapshot(reread)))
        throw new InvalidOperationException("Reserialized typed roundtrip mismatch at case " + count);
    count++;
}
if (ForbiddenRegistry.Calls != 0) throw new InvalidOperationException("Registry access invalidated roundtrip");
Console.WriteLine($"Verified {count} typed Go/kernel -> actual EventSerializer -> actual EventSerializer roundtrips; runtime {Environment.Version}");
Console.WriteLine(typeof(EventSerializer).Assembly.FullName);
Console.WriteLine(typeof(Cratis.Json.EnumConverterFactory).Assembly.FullName);
Console.WriteLine(typeof(JsonSerializer).Assembly.FullName);

static JsonNode? Snapshot(object? value)
{
    if (value is null) return null;
    if (value.GetType().IsEnum)
        return new JsonObject { ["declaredType"] = value.GetType().Name, ["numeric"] = ((IFormattable)value).ToString("D", CultureInfo.InvariantCulture) };
    if (value is IEnumerable sequence)
        return new JsonArray(sequence.Cast<object?>().Select(Snapshot).ToArray());
    var result = new JsonObject();
    foreach (var property in value.GetType().GetProperties()) result.Add(property.Name, Snapshot(property.GetValue(value)));
    return result;
}
