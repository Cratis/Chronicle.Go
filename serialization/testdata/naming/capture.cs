// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#:package Cratis.Chronicle@19.153.0
#:package Cratis.Fundamentals@7.33.1
#:property EnableNETAnalyzers=false
#:property RunAnalyzers=false
#:property PublishAot=false

using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Cratis.Chronicle;
using Cratis.Chronicle.Compliance;
using Cratis.Chronicle.Events;
using Cratis.Chronicle.Schemas;
using Cratis.Serialization;

if (args.Length != 1) throw new ArgumentException("Usage: dotnet run --file capture.cs -- OUTPUT.json (must not exist)");
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
var profiles = new List<object>();
foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
{
    var options = ClientOptions(naming);
    var derived = new EmptyDerived();
    var serializer = (EventSerializer)typeof(EventSerializer).GetConstructors().Single().Invoke([
        DispatchProxy.Create<IClientArtifactsProvider, EmptyArtifacts>(), null,
        DispatchProxy.Create<IEventTypes, ForbiddenRegistry>(), options, derived]);
    var generator = new JsonSchemaGenerator(new EmptyMetadata(), naming, derived);
    foreach (var value in new object[] { new NamingEvent(), new NamingReadModel(), new ExplicitEvent() })
    {
        var type = value.GetType();
        profiles.Add(new
        {
            policy = naming.GetType().Name,
            declaredType = type.Name,
            payload = await serializer.Serialize(value),
            eventSchema = JsonNode.Parse(generator.Generate(type).ToJson()),
            readModelSchema = JsonNode.Parse(generator.GenerateForReadModel(type).ToJson()),
            fields = Fields(type, options, naming)
        });
    }
}
if (ForbiddenRegistry.Calls != 0) throw new InvalidOperationException("Capture accessed the event registry");
var output = new
{
    runtime = Environment.Version.ToString(),
    chronicle = typeof(ChronicleClient).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion,
    fundamentals = typeof(DefaultNamingPolicy).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion,
    profiles
};
using var stream = new FileStream(args[0], FileMode.CreateNew, FileAccess.Write);
JsonSerializer.Serialize(stream, output, new JsonSerializerOptions { WriteIndented = true });
stream.WriteByte((byte)'\n');

static JsonSerializerOptions ClientOptions(INamingPolicy naming)
{
    // Run the real packaged client initializer without constructing a connection
    // or triggering discovery. The same isolation is used by the binary capture.
    const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
    var client = (ChronicleClient)RuntimeHelpers.GetUninitializedObject(typeof(ChronicleClient));
    var options = new ChronicleOptions { AutoDiscoverAndRegister = false };
    typeof(ChronicleClient).GetField("<Options>k__BackingField", flags)!.SetValue(client, options);
    typeof(ChronicleClient).GetField("_namingPolicy", flags)!.SetValue(client, naming);
    typeof(ChronicleClient).GetMethod("InitializeJsonSerializationOptions", flags)!.Invoke(client, null);
    return options.JsonSerializerOptions;
}

static List<object> Fields(Type root, JsonSerializerOptions options, INamingPolicy naming)
{
    // JsonTypeInfo supplies the actual serializer names, including attributes.
    // Separately invoke the packaged projection expression resolver: it does not
    // consult those attributes. Do not reproduce either naming algorithm here.
    var assembly = typeof(ChronicleClient).Assembly;
    var paths = assembly.GetType("Cratis.Chronicle.Projections.ProjectionPropertyPaths")!;
    var role = assembly.GetType("Cratis.Chronicle.Projections.ProjectionPathRole")!;
    var resolve = paths.GetMethod("FromExpression", [typeof(LambdaExpression), typeof(INamingPolicy), role])!;
    var parameter = Expression.Parameter(root, "value");
    var fields = new List<object>();
    Visit(root, parameter, "", "");
    return fields;

    void Visit(Type type, Expression parent, string clrPrefix, string wirePrefix)
    {
        foreach (var property in options.GetTypeInfo(type).Properties)
        {
            var member = (PropertyInfo)property.AttributeProvider!;
            var expression = Expression.Property(parent, member);
            var clrPath = clrPrefix + member.Name;
            var serializedPath = wirePrefix + property.Name;
            var lambda = Expression.Lambda(expression, parameter);
            fields.Add(new
            {
                clrPath,
                name = property.Name,
                serializedPath,
                sourcePath = resolve.Invoke(null, [lambda, naming, Enum.Parse(role, "Source")])!.ToString(),
                targetPath = resolve.Invoke(null, [lambda, naming, Enum.Parse(role, "Target")])!.ToString()
            });
            if (member.PropertyType != typeof(string))
                Visit(member.PropertyType, expression, clrPath + ".", serializedPath + ".");
        }
    }
}

public sealed class NamingEvent
{
    public string URLValue { get; init; } = "url";
    public string ID { get; init; } = "identifier";
    public string Person { get; init; } = "Ada";
    public NamingDetail Nested { get; init; } = new();
}

public sealed class NamingReadModel
{
    public string URLValue { get; init; } = "url";
    public string Id { get; init; } = "identifier";
    public string Person { get; init; } = "Ada";
    public NamingDetail Nested { get; init; } = new();
}

public sealed class NamingDetail
{
    public string URLValue { get; init; } = "nested-url";
    public string Person { get; init; } = "Grace";
}

public sealed class ExplicitEvent
{
    [JsonPropertyName("url_value")]
    public string URLValue { get; init; } = "url";
    [JsonPropertyName("Id")]
    public string Id { get; init; } = "shared-identifier";
    [JsonPropertyName("details")]
    public ExplicitDetail Nested { get; init; } = new();
}

public sealed class ExplicitDetail
{
    [JsonPropertyName("uri")]
    public string URLValue { get; init; } = "nested-url";
    [JsonPropertyName("display_name")]
    public string Person { get; init; } = "Grace";
}

public class EmptyArtifacts : DispatchProxy
{
    protected override object? Invoke(MethodInfo? method, object?[]? args) =>
        method!.ReturnType == typeof(IEnumerable<Type>) ? Array.Empty<Type>() :
        throw new InvalidOperationException("Unexpected artifacts operation: " + method.Name);
}

public class ForbiddenRegistry : DispatchProxy
{
    public static int Calls { get; private set; }
    protected override object? Invoke(MethodInfo? method, object?[]? args)
    {
        Calls++;
        throw new InvalidOperationException("Capture attempted event registry access");
    }
}

public sealed class EmptyDerived : IDerivedTypes
{
    public IEnumerable<Type> TypesWithDerivatives => [];
    public bool HasDerivatives(Type type) => false;
    public bool IsDerivedType(Type type) => false;
    public Type GetDerivedTypeFor(Type targetType, DerivedTypeId id) => throw new InvalidOperationException("No derivatives configured");
    public Type GetTargetTypeFor(Type derivedType) => throw new InvalidOperationException("No derivatives configured");
}

public sealed class EmptyMetadata : IComplianceMetadataResolver
{
    public bool HasMetadataFor(Type type) => false;
    public bool HasMetadataFor(PropertyInfo property) => false;
    public IEnumerable<ComplianceMetadata> GetMetadataFor(Type type) => [];
    public IEnumerable<ComplianceMetadata> GetMetadataFor(PropertyInfo property) => [];
}
