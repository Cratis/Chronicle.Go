// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Collections;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Cratis.Chronicle;
using Cratis.Chronicle.Json;
using Cratis.Chronicle.Schemas;
using Cratis.Json;
using Cratis.Serialization;

if (args.Length != 1) throw new ArgumentException("Usage: BinaryCapture.dll OUTPUT.json (must not exist)");
if (Environment.Version.ToString() != "10.0.12") throw new InvalidOperationException("Requires runtime 10.0.12 exactly");
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
var profiles = new List<object>();
foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
{
    var clientOptions = Isolation.ClientOptions(naming);
    var derived = new EmptyDerived();
    var serializer = Isolation.Serializer(clientOptions, derived);
    var generator = new JsonSchemaGenerator(new EmptyMetadata(), naming, derived);
    var converter = new ExpandoObjectConverter(new TypeFormats());
    var cases = new List<object>();
    var schemas = new List<object>();
    foreach (var type in new[] { typeof(BinaryEvent), typeof(BinaryModel), typeof(BinaryRecord), typeof(BinaryMapControl) })
        foreach (var api in new[] { "Generate", "GenerateForReadModel" })
            schemas.Add(new { declaredType = type.Name, operation = api, result = Observe(() => JsonNode.Parse(Schema(api, type).ToJson())) });
    var samples = new Dictionary<string, BinaryEvent> {
        ["empty"] = new(), ["zero"] = new() { Payload = [0] },
        ["one"] = new() { Payload = [1] }, ["two"] = new() { Payload = [1, 2] },
        ["three"] = new() { Payload = [1, 2, 3] }, ["plus-slash"] = new() { Payload = [0xFB, 0xFF] },
        ["all-bytes"] = new() { Payload = Enumerable.Range(0, 256).Select(x => (byte)x).ToArray() },
        ["null"] = new() { Payload = null!, Chunks = null!, Nested = null! },
        ["optional-empty"] = new() { Optional = [] },
        ["optional-value"] = new() { Optional = [0xFB, 0xFF] },
        ["chunks"] = new() { Chunks = [[], [1], [0xFB, 0xFF]], Nested = new() { Inner = [1, 2] } }
    };
    foreach (var (id, value) in samples)
    {
        var write = await ObserveAsync(async () => (await serializer.Serialize(value)).ToJsonString());
        cases.Add(new { declaredType = "BinaryEvent", id, operation = "EventSerializer.Serialize", input = Snapshot(value), result = write });
        // Capture each direction independently, using actual serialized JSON, not inferred base64.
        var json = (await serializer.Serialize(value)).ToJsonString();
        foreach (var api in new[] { "Generate", "GenerateForReadModel" })
            Expando(api, id, json);
    }
    foreach (var (id, token) in new Dictionary<string, string> {
        ["canonical"] = "\"AQ==\"", ["unpadded"] = "\"AQ\"", ["short-padding"] = "\"AQ=\"",
        ["leading-space"] = "\" AQ==\"", ["trailing-newline"] = "\"AQ==\\n\"",
        ["escaped-plus"] = "\"\\u002B/8=\"", ["url-safe"] = "\"-_8=\"", ["padding-bits"] = "\"AR==\"",
        ["empty"] = "\"\"", ["invalid"] = "\"!!\"", ["null"] = "null",
        ["number"] = "1", ["array"] = "[1,2]", ["bool"] = "true", ["object"] = "{}"
    })
    {
        await Read("payload/" + id, "{\"" + PropertyName("Payload") + "\":" + token + "}");
        await Read("optional/" + id, "{\"" + PropertyName("Optional") + "\":" + token + "}");
        await Read("nested/" + id, "{\"" + PropertyName("Nested") + "\":{\"" + PropertyName("Inner") + "\":" + token + "}}");
    }
    await Read("missing", "{}");
    foreach (var (id, token) in new Dictionary<string, string> { ["null"] = "null", ["empty"] = "[]", ["values"] = "[\"\",\"AQ==\"]", ["null-element"] = "[null]", ["array-element"] = "[[1]]" })
        await Read("chunks/" + id, "{\"" + PropertyName("Chunks") + "\":" + token + "}");
    profiles.Add(new {
        namingPolicy = naming.GetType().FullName, clientOptions = Options(clientOptions),
        eventOptions = Options(Isolation.SerializerOptions(serializer)), schemaOptions = Options(Isolation.SchemaOptions(generator)), schemas, cases
    });
    string PropertyName(string name) => naming.JsonPropertyNamingPolicy?.ConvertName(name) ?? name;
    JsonSchema Schema(string api, Type type) => api == "Generate" ? generator.Generate(type) : generator.GenerateForReadModel(type);
    async Task Read(string id, string input)
    {
        object result;
        try {
            var value = await serializer.Deserialize(typeof(BinaryEvent), JsonNode.Parse(input)!.AsObject());
            result = new { status = "accepted", output = Snapshot(value), reserialize = await ObserveAsync(async () => (await serializer.Serialize(value)).ToJsonString()) };
        } catch (Exception error) { result = Failure(error); }
        cases.Add(new { declaredType = "BinaryEvent", id, operation = "EventSerializer.Deserialize", input, result });
    }
    void Expando(string api, string id, string input)
    {
        object result;
        try {
            var schema = Schema(api, typeof(BinaryEvent));
            var value = converter.ToExpandoObject(JsonNode.Parse(input)!.AsObject(), schema);
            result = new { status = "accepted", output = Snapshot(value), toJson = Observe(() => converter.ToJsonObject(value, schema).ToJsonString()) };
        } catch (Exception error) { result = Failure(error); }
        cases.Add(new { declaredType = "BinaryEvent", id, operation = "ExpandoObjectConverter.ToExpandoObject/ToJsonObject", schemaAPI = api, input, result });
    }
}
if (ForbiddenRegistry.Calls != 0) throw new InvalidOperationException("Registry access invalidated capture");
var fixture = new {
    profile = "chronicle-19.29.4_fundamentals-7.19.6_net-10.0.12",
    packages = new { chronicle = "19.29.4", fundamentals = "7.19.6" },
    runtime = new { framework = RuntimeInformation.FrameworkDescription, version = Environment.Version.ToString(), architecture = RuntimeInformation.ProcessArchitecture.ToString(), culture = CultureInfo.CurrentCulture.Name },
    assemblies = new[] { typeof(ChronicleClient).Assembly, typeof(EnumConverterFactory).Assembly, typeof(JsonSerializer).Assembly, typeof(JsonSchema).Assembly }.Distinct().Select(a => new { name = a.FullName, informationalVersion = a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion }),
    isolation = new { registryCalls = ForbiddenRegistry.Calls, artifacts = "empty", derivedTypes = "empty explicit catalog", network = "no client connection constructed", bson = "not captured; in-memory Expando only" }, profiles
};
using (var stream = new FileStream(args[0], FileMode.CreateNew, FileAccess.Write))
    stream.Write(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(fixture, new JsonSerializerOptions { WriteIndented = true }) + "\n"));
Console.WriteLine("Captured binary profile with runtime " + Environment.Version);

static object Options(JsonSerializerOptions options) => new {
    propertyNamingPolicy = options.PropertyNamingPolicy?.GetType().FullName, options.PropertyNameCaseInsensitive,
    defaultIgnoreCondition = options.DefaultIgnoreCondition.ToString(), options.WriteIndented,
    encoder = options.Encoder?.GetType().FullName ?? "System.Text.Encodings.Web.JavaScriptEncoder.Default",
    typeInfoResolver = options.TypeInfoResolver?.GetType().FullName, converters = options.Converters.Select(c => c.GetType().FullName).ToArray()
};
static object? Snapshot(object? value)
{
    if (value is null) return null;
    if (value is byte[] bytes) return new { declaredType = "System.Byte[]", hex = Convert.ToHexString(bytes) };
    if (value is IDictionary<string, object?> dictionary) return dictionary.ToDictionary(p => p.Key, p => Snapshot(p.Value));
    if (value is string || value.GetType().IsPrimitive) return new { declaredType = value.GetType().FullName, value };
    if (value is IEnumerable sequence) return sequence.Cast<object?>().Select(Snapshot).ToArray();
    return value.GetType().GetProperties().ToDictionary(p => p.Name, p => Snapshot(p.GetValue(value)));
}
static object Failure(Exception error)
{
    while (error is TargetInvocationException { InnerException: not null }) error = error.InnerException;
    return new { status = "error", category = error.GetType().FullName, innerCategory = error.InnerException?.GetType().FullName };
}
static object Observe(Func<object?> action)
{
    try { return new { status = "accepted", output = action() }; }
    catch (Exception error) { return Failure(error); }
}
static async Task<object> ObserveAsync(Func<Task<string>> action)
{
    try { return new { status = "accepted", output = await action() }; }
    catch (Exception error) { return Failure(error); }
}
