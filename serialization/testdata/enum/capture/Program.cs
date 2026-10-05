// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Collections;
using System.Dynamic;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Cratis.Chronicle;
using Cratis.Chronicle.Events;
using Cratis.Chronicle.Json;
using Cratis.Chronicle.Schemas;
using Cratis.Json;
using Cratis.Serialization;

if (args.Length != 1) throw new ArgumentException("Usage: EnumCapture.dll OUTPUT.json (must not exist)");
if (Environment.Version.ToString() != "10.0.12") throw new InvalidOperationException("Requires runtime 10.0.12 exactly");
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
var enumTypes = new[] { typeof(Plain), typeof(NoZero), typeof(Bits), typeof(ByteEnum), typeof(UIntEnum), typeof(LongEnum), typeof(Int32Sample), typeof(AllBits) };
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
    var types = enumTypes.SelectMany(t => new[] { typeof(Scalar<>).MakeGenericType(t), typeof(NullableScalar<>).MakeGenericType(t), typeof(ArrayValue<>).MakeGenericType(t) })
        .Concat([typeof(Int32Defaults), typeof(NamingControl), typeof(ConceptDefaultControl)]).ToArray();
    foreach (var type in types)
    {
        foreach (var operation in new[] { "Generate", "GenerateForReadModel" })
            schemas.Add(new { declaredType = TypeName(type), operation, result = Observe(() => JsonNode.Parse(Schema(operation, type).ToJson())) });
    }
    foreach (var enumType in enumTypes)
    {
        var scalar = typeof(Scalar<>).MakeGenericType(enumType);
        var nullable = typeof(NullableScalar<>).MakeGenericType(enumType);
        var array = typeof(ArrayValue<>).MakeGenericType(enumType);
        var key = PropertyName("Value");
        var arrayKey = PropertyName("Values");
        var inputs = new[] { "0", "1", "2", "3", "4", "5", "7", "8", "9", "-1", "-2", "2147483647", "-2147483648", "2147483648", "-2147483649", "\"One\"", "\"one\"", "\"ONE\"", "\" One \"", "\"Missing\"", "\"9\"", "\"+1\"", "\"01\"", "\" 9 \"", "\"2147483648\"", "\"-2147483648\"", "\"One, Two\"", "\"one, two\"", "\"One,One\"", "\"1, 2\"", "\"One,\"", "\"\"", "\" \"", "null", "true", "false", "[]", "{}", "1.0", "1e0", "1E+0", "0.5", "\"1.0\"", "\"1e0\"", "-0", "\"-0\"", "\"A, B\"", "\"A, C\"", "\"a, c\"", "\"5\"", "\"8\"", "\"A, Missing\"", "\"All\"", "\"-1\"" };
        foreach (var input in inputs.Concat(Enum.GetNames(enumType).Select(n => JsonSerializer.Serialize(n))).Distinct())
            await Read(scalar, "token:" + input, "{" + JsonSerializer.Serialize(key) + ":" + input + "}");
        await Read(scalar, "missing", "{}");
        await Read(scalar, "case-insensitive-property", "{\"vAlUe\":1}");
        foreach (var (id, input) in new[] { ("missing", "{}"), ("null", "{\"" + key + "\":null}"), ("one", "{\"" + key + "\":1}"), ("unknown", "{\"" + key + "\":9}") })
            await Read(nullable, id, input);
        foreach (var (id, input) in new[] { ("missing", "{}"), ("null", "{\"" + arrayKey + "\":null}"), ("empty", "{\"" + arrayKey + "\":[]}"), ("numbers", "{\"" + arrayKey + "\":[0,1]}"), ("names", "{\"" + arrayKey + "\":[\"One\",\"one\"]}"), ("null-element", "{\"" + arrayKey + "\":[null]}"), ("unknown", "{\"" + arrayKey + "\":[9]}"), ("not-array", "{\"" + arrayKey + "\":1}") })
            await Read(array, id, input);
        await Read(array, "declared-names", "{\"" + arrayKey + "\":" + JsonSerializer.Serialize(Enum.GetNames(enumType)) + "}");
        foreach (var number in new long[] { 0, 1, 3, 5, 7, 8, 9, -1, int.MinValue, int.MaxValue }
            .Concat(Enum.GetValues(enumType).Cast<object>().Select(Convert.ToInt64)).Distinct())
        {
            // Wide historical declarations are captured as controls; no wrapping inputs into Byte/UInt.
            if (enumType == typeof(ByteEnum) && (number < 0 || number > 255) || enumType == typeof(UIntEnum) && number < 0) continue;
            var value = Enum.ToObject(enumType, number);
            await Write(scalar, "numeric:" + number.ToString(CultureInfo.InvariantCulture), Activator.CreateInstance(scalar, value)!);
        }
        await Write(nullable, "null", Activator.CreateInstance(nullable, new object?[] { null })!);
        await Write(array, "null", Activator.CreateInstance(array, new object?[] { null })!);
        await Write(array, "empty", Activator.CreateInstance(array, Array.CreateInstance(enumType, 0))!);
        var values = Array.CreateInstance(enumType, 2);
        values.SetValue(Enum.ToObject(enumType, 0), 0);
        values.SetValue(Enum.ToObject(enumType, 1), 1);
        await Write(array, "zero-one", Activator.CreateInstance(array, values)!);
        await Write(array, "declared-values", Activator.CreateInstance(array, Enum.GetValues(enumType))!);
    }
    await Read(typeof(Int32Defaults), "missing", "{}");
    await Read(typeof(Int32Defaults), "explicit-null-value", "{\"" + PropertyName("Value") + "\":null}");
    await Read(typeof(Int32Defaults), "explicit-null-optionals", "{\"" + PropertyName("Optional") + "\":null,\"" + PropertyName("Values") + "\":null}");
    await Write(typeof(Int32Defaults), "constructor-defaults", new Int32Defaults());
    await Write(typeof(NamingControl), "tag-and-acronym", new NamingControl(Int32Sample.Negative, Int32Sample.One));
    await Read(typeof(NamingControl), "tag-and-acronym", "{\"" + PropertyName("URLValue") + "\":\"Negative\",\"explicit_enum\":\"One\"}");
    await Read(typeof(ConceptDefaultControl), "missing", "{}");
    await Read(typeof(ConceptDefaultControl), "explicit-null", "{\"" + PropertyName("Value") + "\":null}");
    await Write(typeof(ConceptDefaultControl), "constructor-default", new ConceptDefaultControl());
    await Write(typeof(ConceptDefaultControl), "declared-one", new ConceptDefaultControl(new PlainConcept(Plain.One)));

    // Execute the actual packaged JSON/Expando converter in both directions. These
    // are in-memory schema-direction observations, never Mongo/BSON or live-kernel proof.
    foreach (var type in new[] { typeof(Scalar<Int32Sample>), typeof(Scalar<Bits>), typeof(Scalar<AllBits>), typeof(NullableScalar<Int32Sample>), typeof(ArrayValue<Int32Sample>), typeof(ArrayValue<Bits>), typeof(Int32Defaults), typeof(ConceptDefaultControl) })
    {
        var isArray = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ArrayValue<>);
        var key = PropertyName(isArray ? "Values" : "Value");
        foreach (var api in new[] { "Generate", "GenerateForReadModel" })
        {
            var schema = Schema(api, type);
            foreach (var token in isArray ? new[] { "[]", "[0,1]", "[3,5,7,8,-1]", "[\"One\"]", "[null]", "null" } : new[] { "0", "1", "3", "5", "7", "8", "9", "-1", "-2147483648", "2147483647", "\"One\"", "\"one\"", "\"A, C\"", "null" })
                Expando(type, api, schema, "token:" + token, "{\"" + key + "\":" + token + "}");
            Expando(type, api, schema, "missing", "{}");
        }
    }
    profiles.Add(new {
        namingPolicy = naming.GetType().FullName,
        clientOptions = Options(clientOptions), eventOptions = Options(Isolation.SerializerOptions(serializer)), schemaOptions = Options(Isolation.SchemaOptions(generator)),
        declarations = types.Select(t => new { type = TypeName(t), properties = t.GetProperties().Select(p => new { name = p.Name, declaredType = TypeName(p.PropertyType), jsonName = p.GetCustomAttribute<System.Text.Json.Serialization.JsonPropertyNameAttribute>()?.Name ?? PropertyName(p.Name) }) }),
        schemas, cases
    });

    string PropertyName(string name) => naming.JsonPropertyNamingPolicy?.ConvertName(name) ?? name;
    JsonSchema Schema(string operation, Type type) => operation == "Generate" ? generator.Generate(type) : generator.GenerateForReadModel(type);
    async Task Read(Type type, string id, string input)
    {
        object result;
        try
        {
            var value = await serializer.Deserialize(type, JsonNode.Parse(input)!.AsObject());
            result = new { status = "accepted", output = Snapshot(value), reserialize = await ObserveAsync(async () => (await serializer.Serialize(value)).ToJsonString()) };
        }
        catch (Exception error) { result = Failure(error); }
        cases.Add(new { declaredType = TypeName(type), id, operation = "EventSerializer.Deserialize", input, result });
    }
    async Task Write(Type type, string id, object value) => cases.Add(new { declaredType = TypeName(type), id, operation = "EventSerializer.Serialize", input = Snapshot(value), result = await ObserveAsync(async () => (await serializer.Serialize(value)).ToJsonString()) });
    void Expando(Type type, string api, JsonSchema schema, string id, string input)
    {
        object result;
        try
        {
            var expando = converter.ToExpandoObject(JsonNode.Parse(input)!.AsObject(), schema);
            result = new { status = "accepted", output = Snapshot(expando), toJson = Observe(() => converter.ToJsonObject(expando, schema).ToJsonString()) };
        }
        catch (Exception error) { result = Failure(error); }
        cases.Add(new { declaredType = TypeName(type), id, operation = "ExpandoObjectConverter.ToExpandoObject/ToJsonObject", schemaAPI = api, input, result });
    }
}
if (ForbiddenRegistry.Calls != 0) throw new InvalidOperationException("Registry access invalidated capture");
var fixture = new {
    profile = "chronicle-19.29.4_fundamentals-7.19.6_net-10.0.12",
    goAdmission = "not-implemented; capture only, no Go API approved",
    packages = new { chronicle = "19.29.4", fundamentals = "7.19.6" },
    runtime = new { framework = RuntimeInformation.FrameworkDescription, version = Environment.Version.ToString(), architecture = RuntimeInformation.ProcessArchitecture.ToString(), culture = CultureInfo.CurrentCulture.Name },
    assemblies = new[] { typeof(ChronicleClient).Assembly, typeof(EnumConverterFactory).Assembly, typeof(JsonSerializer).Assembly, typeof(JsonSchema).Assembly, typeof(ExpandoObjectConverter).Assembly }.Distinct().Select(a => new { name = a.FullName, informationalVersion = a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion }),
    isolation = new { options = "packaged ChronicleClient.InitializeJsonSerializationOptions via reflection on uninitialized client; no client constructor", serializer = "actual EventSerializer constructor and Serialize/Deserialize(Type,JsonObject)", artifacts = "empty", derivedTypes = "empty explicit catalog", registryCalls = ForbiddenRegistry.Calls, network = "no client connection constructed", bson = "not captured; no Mongo converter in this harness" },
    enums = enumTypes.Select(t => new { name = t.Name, sourceGroup = t == typeof(Int32Sample) || t == typeof(AllBits) ? "Chronicle control" : "historical declarations unchanged", underlyingType = Enum.GetUnderlyingType(t).FullName, flags = t.IsDefined(typeof(FlagsAttribute), false), members = Enum.GetNames(t).Select(n => new { name = n, numeric = Numeric(Enum.Parse(t, n)) }) }),
    profiles
};
using (var stream = new FileStream(args[0], FileMode.CreateNew, FileAccess.Write))
{
    var bytes = System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(fixture, new JsonSerializerOptions { WriteIndented = true }) + "\n");
    stream.Write(bytes);
}
Console.WriteLine("Captured both naming policies, actual EventSerializer, both schema APIs and packaged Expando directions; runtime " + Environment.Version);

static object Options(JsonSerializerOptions options) => new {
    propertyNamingPolicy = options.PropertyNamingPolicy?.GetType().FullName,
    options.PropertyNameCaseInsensitive, defaultIgnoreCondition = options.DefaultIgnoreCondition.ToString(), options.WriteIndented,
    encoder = options.Encoder?.GetType().FullName ?? "System.Text.Encodings.Web.JavaScriptEncoder.Default",
    typeInfoResolver = options.TypeInfoResolver?.GetType().FullName,
    converters = options.Converters.Select(c => c.GetType().FullName).ToArray()
};
static string TypeName(Type type) => type.IsArray ? TypeName(type.GetElementType()!) + "[]" : type.IsGenericType ? type.Name.Split('`')[0] + "<" + string.Join(",", type.GenericTypeArguments.Select(TypeName)) + ">" : type.FullName!;
static string Numeric(object value) => ((IFormattable)value).ToString("D", CultureInfo.InvariantCulture);
static object? Snapshot(object? value)
{
    if (value is null) return null;
    if (value.GetType().IsEnum) return new { declaredType = TypeName(value.GetType()), numeric = Numeric(value) };
    if (value is IDictionary<string, object?> dictionary) return dictionary.ToDictionary(p => p.Key, p => Snapshot(p.Value));
    if (value is string || value.GetType().IsPrimitive) return new { declaredType = TypeName(value.GetType()), value };
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
