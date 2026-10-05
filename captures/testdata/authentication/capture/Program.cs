// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Cratis.Chronicle;
using Cratis.Chronicle.Captures;
using Cratis.Chronicle.Concepts.Captures;
using Cratis.Chronicle.Connections;
using Cratis.Chronicle.Contracts;
using Cratis.Chronicle.Events;
using Cratis.Chronicle.Webhooks;
using Cratis.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using ContractWebhooks = Cratis.Chronicle.Contracts.Observation.Webhooks.IWebhooks;

try
{
    if (args.Length != 3 || Environment.Version.ToString() != "10.0.12") throw new InvalidOperationException();
    CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
    var cases = new List<object>();
    var configurations = new List<object>();
    foreach (var naming in new INamingPolicy[] { new DefaultNamingPolicy(), new CamelCaseNamingPolicy() })
    {
        var policy = naming.GetType().Name;
        var artifacts = Isolation.Artifacts();
        using var client = new ChronicleClient(Isolation.Connection(), new ChronicleOptions { AutoDiscoverAndRegister = false }, artifactsProvider: artifacts, serviceProvider: new FixedProvider(), loggerFactory: NullLoggerFactory.Instance, namingPolicy: naming);
        Calls.Count("ChronicleClient.publicBorrowedConstructor");
        var options = client.Options.JsonSerializerOptions;
        var serializer = Isolation.Serializer(options, artifacts);
        configurations.Add(new {
            namingPolicy = policy,
            propertyNamingPolicy = options.PropertyNamingPolicy?.GetType().FullName,
            options.PropertyNameCaseInsensitive,
            defaultIgnoreCondition = options.DefaultIgnoreCondition.ToString(),
            options.WriteIndented,
            converters = options.Converters.Select(c => c.GetType().FullName).ToArray(),
            typeInfoResolver = options.TypeInfoResolver?.GetType().FullName,
            encoder = options.Encoder?.GetType().FullName ?? "System.Text.Encodings.Web.JavaScriptEncoder.Default"
        });
        foreach (var id in new[] { "default-null", "explicit-none", "basic", "bearer", "oauth", "basic-then-bearer", "bearer-then-basic", "basic-then-oauth" })
        {
            var builder = new WebhookSourceBuilder("/synthetic-capture");
            switch (id)
            {
                case "basic": builder.WithBasicAuth("synthetic-user", "synthetic-password"); break;
                case "bearer": builder.WithBearerToken("synthetic-token"); break;
                case "oauth": builder.WithOAuth("https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret"); break;
                case "basic-then-bearer": builder.WithBasicAuth("synthetic-user", "synthetic-password").WithBearerToken("synthetic-token"); break;
                case "bearer-then-basic": builder.WithBearerToken("synthetic-token").WithBasicAuth("synthetic-user", "synthetic-password"); break;
                case "basic-then-oauth": builder.WithBasicAuth("synthetic-user", "synthetic-password").WithOAuth("https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret"); break;
            }
            var source = builder.Build();
            if (id == "explicit-none") source = source with { Authorization = SourceAuthorization.None };
            var raw = (await serializer.Serialize(source)).ToJsonString();
            cases.Add(new { surface = "inbound", namingPolicy = policy, operation = "EventSerializer.Serialize", id, evidence = "public-actual-serialization", rawJSON = raw, authorizationArm = Arm(source.Authorization), read = await ReadSource(raw) });
        }
        // Actual converter serialization controls, not outgoing Register factories.
        foreach (var (id, authorization) in new (string, SourceAuthorization)[] {
            ("none", SourceAuthorization.None),
            ("basic", new SourceBasicAuthorization("synthetic-user", "synthetic-password")),
            ("bearer", new SourceBearerTokenAuthorization("synthetic-token")),
            ("oauth", new SourceOAuthAuthorization("https://synthetic.invalid/oauth", "synthetic-client", "synthetic-secret")) })
        {
            var raw = JsonSerializer.Serialize(authorization, options);
            var read = Observe(() => JsonSerializer.Deserialize<SourceAuthorization>(raw, options), value => Arm((SourceAuthorization?)value), value => JsonSerializer.Serialize((SourceAuthorization?)value, options));
            cases.Add(new { surface = "inbound", namingPolicy = policy, operation = "JsonSerializer.SourceAuthorization", id, evidence = "public-actual-serialization", rawJSON = raw, authorizationArm = Arm(authorization), read });
        }
        var controls = new (string, string)[] {
            ("missing-discriminator", "{}"), ("unknown-discriminator", "{\"type\":\"unrecognized\"}"),
            ("case-changed-value", "{\"type\":\"Basic\",\"username\":\"synthetic-user\",\"password\":\"synthetic-password\"}"),
            ("case-changed-key", "{\"Type\":\"basic\",\"username\":\"synthetic-user\",\"password\":\"synthetic-password\"}"),
            ("basic-missing-username", "{\"type\":\"basic\",\"password\":\"synthetic-password\"}"),
            ("basic-missing-password", "{\"type\":\"basic\",\"username\":\"synthetic-user\"}"),
            ("bearer-missing-token", "{\"type\":\"bearer\"}"),
            ("oauth-missing-authority", "{\"type\":\"oauth\",\"clientId\":\"synthetic-client\",\"clientSecret\":\"synthetic-secret\"}"),
            ("oauth-missing-client-id", "{\"type\":\"oauth\",\"authority\":\"https://synthetic.invalid/oauth\",\"clientSecret\":\"synthetic-secret\"}"),
            ("oauth-missing-client-secret", "{\"type\":\"oauth\",\"authority\":\"https://synthetic.invalid/oauth\",\"clientId\":\"synthetic-client\"}"),
            ("null-authorization", "null"), ("explicit-none", "{\"type\":\"none\"}") };
        foreach (var (id, auth) in controls)
        {
            var authKey = naming.JsonPropertyNamingPolicy?.ConvertName("Authorization") ?? "Authorization";
            var input = "{\"" + (naming.JsonPropertyNamingPolicy?.ConvertName("Type") ?? "Type") + "\":\"Webhook\",\"" + authKey + "\":" + auth + "}";
            cases.Add(new { surface = "inbound", namingPolicy = policy, operation = "EventSerializer.Deserialize", id, evidence = "public-actual-serialization", rawJSON = input, read = await ReadSource(input) });
        }
        async Task<object> ReadSource(string raw)
        {
            try
            {
                var decoded = (SourceDefinition)await serializer.Deserialize(typeof(SourceDefinition), JsonNode.Parse(raw)!.AsObject());
                object secondary;
                try { secondary = new { status = "accepted", rawJSON = (await serializer.Serialize(decoded)).ToJsonString() }; }
                catch (Exception error) { secondary = Failure(error); }
                return new { status = "accepted", outputType = decoded.GetType().FullName, authorizationArm = Arm(decoded.Authorization), reserialize = secondary };
            }
            catch (Exception error) { return new { status = "error", category = Category(error), reserialize = new { status = "not-attempted", reason = "deserialize-failed" } }; }
        }
    }
    var invoker = new RecordingInvoker();
    var proxy = new InProcessAwareGrpcClientProxiesClientFactory().CreateClient<ContractWebhooks>(invoker);
    Calls.Count("factory.CreateClient");
    var services = FixedProxy.For<IServices>((method, _) => {
        if (method.Name != "get_Webhooks") throw Calls.Refuse("services." + method.Name);
        Calls.Count("services.Webhooks"); return proxy;
    });
    var connection = Isolation.Connection(services);
    var store = FixedProxy.For<IEventStore>((method, _) => {
        switch (method.Name) {
            case "get_Connection": Calls.Count("store.Connection"); return connection;
            case "get_Name": Calls.Count("store.Name"); return new EventStoreName("synthetic-store");
            default: throw Calls.Refuse("store." + method.Name);
        }
    });
    var webhooks = new Webhooks(Isolation.Catalog(), store, NullLogger<Webhooks>.Instance);
    foreach (var id in new[] { "none-default", "basic-default", "bearer-selected", "basic-then-bearer", "none-false", "bearer-selected-false" })
    {
        var before = invoker.Requests.Count;
        await webhooks.Register(new WebhookId("synthetic-webhook"), new WebhookTargetUrl("https://synthetic.invalid/receive"), builder => {
            if (id.StartsWith("basic", StringComparison.Ordinal)) builder.WithBasicAuth("synthetic-user", "synthetic-password");
            if (id.Contains("bearer", StringComparison.Ordinal)) builder.WithBearerToken("synthetic-token");
            if (id.Contains("selected", StringComparison.Ordinal)) builder.WithEventType<SelectedEvent>().WithHeader("X-Synthetic", "fixed-header");
            if (id.EndsWith("false", StringComparison.Ordinal)) builder.NotActive().NotReplayable();
        });
        if (invoker.Requests.Count != before + 1) throw new InvalidOperationException();
        cases.Add(new { surface = "outgoing", namingPolicy = "not-applicable", operation = "Webhooks.Register", id, evidence = "public-Register-captured", result = invoker.Requests[^1] });
    }
    if (typeof(IWebhookDefinitionBuilder).GetMethods().Any(m => m.Name.Contains("OAuth", StringComparison.Ordinal))) throw new InvalidOperationException();
    cases.Add(new { surface = "outgoing", namingPolicy = "not-applicable", operation = "Webhooks.Register", id = "oauth-unavailable", evidence = "public-api-unavailable", status = "unavailable", reason = "no-public-OAuth-Register-builder" });
    Calls.Verify();
    var capture = new {
        profile = "authentication_chronicle-19.29.4_fundamentals-7.19.6_net-10.0.12",
        sourceAuthority = "2e31b0dfba489159b3db323238f16d0f277056b4",
        packageSource = "ae5e00a8abaa688138b2c2f689e2b4659cccb4fd",
        syntheticInputsOnly = true,
        limits = new[] { "no-encryption", "no-authentication-enforcement", "no-token-acquisition", "no-HTTP-delivery", "no-persistence", "no-kernel", "no-Go-inbound-auth-implementation" },
        runtime = new { framework = RuntimeInformation.FrameworkDescription, version = Environment.Version.ToString(), architecture = RuntimeInformation.ProcessArchitecture.ToString(), culture = CultureInfo.CurrentCulture.Name },
        packages = new[] { ("cratis.chronicle", "19.29.4"), ("cratis.chronicle.connections", "19.29.4"), ("cratis.fundamentals", "7.19.6") }.Select(p => new { name = p.Item1, version = p.Item2, sha256 = Hash(Path.Combine(args[2], p.Item1, p.Item2, p.Item1 + "." + p.Item2 + ".nupkg")) }).ToArray(),
        proxy = new { type = proxy.GetType().FullName, assembly = proxy.GetType().Assembly.GetName().Name, dynamicAssembly = proxy.GetType().Assembly.IsDynamic, fileHash = "not-applicable-dynamic-proxy-generated-by-public-factory" },
        assemblies = new[] { typeof(ChronicleClient).Assembly, typeof(InProcessAwareGrpcClientProxiesClientFactory).Assembly, typeof(ContractWebhooks).Assembly, typeof(INamingPolicy).Assembly, typeof(JsonSerializer).Assembly, typeof(Grpc.Core.CallInvoker).Assembly, typeof(ProtoBuf.Serializer).Assembly, typeof(ProtoBuf.Grpc.Configuration.ClientFactory).Assembly }.Distinct().Select(a => new { name = a.GetName().Name, version = a.GetName().Version?.ToString(), informationalVersion = a.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion, sha256 = Hash(a.Location) }).ToArray(),
        harness = new[] { "Program.cs", "Isolation.cs", "AuthenticationCapture.csproj", "global.json", "packages.lock.json" }.Select(n => new { name = n, sha256 = Hash(Path.Combine(args[1], n)) }).ToArray(),
        allowedCalls = Calls.Allowed, forbiddenCalls = Calls.Forbidden,
        configurations, cases
    };
    using var stream = new FileStream(args[0], FileMode.CreateNew, FileAccess.Write);
    JsonSerializer.Serialize(stream, capture, new JsonSerializerOptions { WriteIndented = true });
    stream.WriteByte((byte)'\n');
    Console.WriteLine("capture cases=" + cases.Count + " forbidden=" + Calls.Forbidden.Count);
    Console.WriteLine(JsonSerializer.Serialize(Calls.Allowed));
    return 0;
}
catch (Exception error)
{
    Console.Error.WriteLine("capture-invalid category=" + Category(error));
    Console.Error.WriteLine(JsonSerializer.Serialize(new { allowedCalls = Calls.Allowed, forbiddenCalls = Calls.Forbidden }));
    return 1;
}
static string Hash(string path) => Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path)));
static string Arm(SourceAuthorization? authorization) => authorization is null ? "null" : authorization.Match(_ => "basic", _ => "bearer", _ => "oauth", _ => "none");
static string Category(Exception error)
{
    while (error is TargetInvocationException { InnerException: not null }) error = error.InnerException;
    return error.GetType().FullName!;
}
static object Failure(Exception error) => new { status = "error", category = Category(error) };
static object Observe(Func<object?> read, Func<object?, string> arm, Func<object?, string> write)
{
    try
    {
        var value = read();
        object secondary;
        try { secondary = new { status = "accepted", rawJSON = write(value) }; }
        catch (Exception error) { secondary = Failure(error); }
        return new { status = "accepted", outputType = value?.GetType().FullName, authorizationArm = arm(value), reserialize = secondary };
    }
    catch (Exception error) { return new { status = "error", category = Category(error), reserialize = new { status = "not-attempted", reason = "deserialize-failed" } }; }
}
