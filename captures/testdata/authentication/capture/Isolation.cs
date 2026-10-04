// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Collections.Immutable;
using System.Reflection;
using System.Text.Json;
using Cratis.Chronicle;
using Cratis.Chronicle.Connections;
using Cratis.Chronicle.Contracts;
using Cratis.Chronicle.Events;
using Cratis.Serialization;
using Cratis.Types;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;

public static class Calls
{
    public static SortedDictionary<string, int> Allowed { get; } = new();
    public static SortedDictionary<string, int> Forbidden { get; } = new();
    public static void Count(string key) => Allowed[key] = Allowed.GetValueOrDefault(key) + 1;
    public static void Verify()
    {
        var expected = new SortedDictionary<string, int> {
            ["artifacts.get_AdditionalEventInformationProviders"] = 2,
            ["catalog.All"] = 4, ["catalog.GetEventTypeFor"] = 2,
            ["ChronicleClient.publicBorrowedConstructor"] = 2,
            ["connection.Dispose"] = 2, ["connection.Services"] = 6,
            ["derived.HasDerivatives"] = 10, ["EventSerializer.publicConstructor"] = 2,
            ["factory.CreateClient"] = 1, ["marshaller.Deserialize"] = 6, ["marshaller.Serialize"] = 6,
            ["provider.IServiceProviderIsService"] = 2, ["provider.IsService.ITypes"] = 2, ["provider.ITypes"] = 2,
            ["rpc.AddWebhooks"] = 6, ["rpc.Dispose"] = 6, ["services.Webhooks"] = 6,
            ["store.Connection"] = 1, ["store.Name"] = 6, ["types.FindMultiple"] = 8
        };
        if (Forbidden.Count != 0 || !Allowed.SequenceEqual(expected)) throw new ForbiddenOperation();
    }
    public static Exception Refuse(string key)
    {
        Forbidden[key] = Forbidden.GetValueOrDefault(key) + 1;
        return new ForbiddenOperation();
    }
}
public sealed class ForbiddenOperation : Exception;

// Explicit fixed collaborators only. A caught refusal still invalidates the whole run.
public class FixedProxy : DispatchProxy
{
    public Func<MethodInfo, object?[]?, object?> Handler { get; set; } = null!;
    protected override object? Invoke(MethodInfo? method, object?[]? args) => Handler(method!, args);
    public static T For<T>(Func<MethodInfo, object?[]?, object?> handler) where T : class
    {
        var proxy = Create<T, FixedProxy>();
        ((FixedProxy)(object)proxy).Handler = handler;
        return proxy;
    }
}
public interface IBorrowedConnection : IChronicleConnection, IChronicleServicesAccessor;

public sealed class FixedProvider : IServiceProvider, IServiceProviderIsService
{
    public bool IsService(Type type)
    {
        if (type != typeof(ITypes)) throw Calls.Refuse("provider.IsService." + type.Name);
        Calls.Count("provider.IsService.ITypes");
        return true;
    }
    readonly ITypes _types = FixedProxy.For<ITypes>((method, _) =>
    {
        if (method.Name == "FindMultiple" || method.Name == "get_All")
        {
            Calls.Count("types." + method.Name);
            return Array.Empty<Type>();
        }
        throw Calls.Refuse("types." + method.Name);
    });
    public object? GetService(Type type)
    {
        if (type == typeof(IServiceProviderIsService))
        {
            Calls.Count("provider.IServiceProviderIsService");
            return this;
        }
        if (type == typeof(ITypes))
        {
            Calls.Count("provider.ITypes");
            return _types;
        }
        throw Calls.Refuse("provider." + type.Name);
    }
}
public sealed class EmptyDerived : IDerivedTypes
{
    public IEnumerable<Type> TypesWithDerivatives { get { Calls.Count("derived.TypesWithDerivatives"); return []; } }
    public bool HasDerivatives(Type type) { Calls.Count("derived.HasDerivatives"); return false; }
    public bool IsDerivedType(Type type) { Calls.Count("derived.IsDerivedType"); return false; }
    public Type GetDerivedTypeFor(Type targetType, DerivedTypeId id) => throw Calls.Refuse("derived.GetDerivedTypeFor");
    public Type GetTargetTypeFor(Type derivedType) => throw Calls.Refuse("derived.GetTargetTypeFor");
}
public static class Isolation
{
    public static IClientArtifactsProvider Artifacts() => FixedProxy.For<IClientArtifactsProvider>((method, _) =>
    {
        if (method.Name == "get_AdditionalEventInformationProviders" && method.ReturnType == typeof(IEnumerable<Type>))
        {
            Calls.Count("artifacts." + method.Name);
            return Array.Empty<Type>();
        }
        throw Calls.Refuse("artifacts." + method.Name);
    });
    public static IBorrowedConnection Connection(IServices? services = null) => FixedProxy.For<IBorrowedConnection>((method, _) =>
    {
        if (method.Name == "get_Services" && services is not null)
        {
            Calls.Count("connection.Services");
            return services;
        }
        if (method.Name == "Dispose") { Calls.Count("connection.Dispose"); return null; }
        throw Calls.Refuse("connection." + method.Name);
    });
    public static IEventTypes NoRegistry() => FixedProxy.For<IEventTypes>((method, _) => throw Calls.Refuse("registry." + method.Name));
    public static EventSerializer Serializer(JsonSerializerOptions options, IClientArtifactsProvider artifacts)
    {
        // The constructor is public; its activator's type is not exposed by the reference
        // assembly. Null is safe only with the explicitly empty provider list above.
        var ctor = typeof(EventSerializer).GetConstructors().Single();
        Calls.Count("EventSerializer.publicConstructor");
        return (EventSerializer)ctor.Invoke([artifacts, null, NoRegistry(), options, new EmptyDerived()]);
    }
    public static readonly EventType First = new(new EventTypeId("capture-first"), new EventTypeGeneration(2));
    public static readonly EventType Second = new(new EventTypeId("capture-second"), new EventTypeGeneration(3));
    public static IEventTypes Catalog() => FixedProxy.For<IEventTypes>((method, args) =>
    {
        if (method.Name == "get_All")
        {
            Calls.Count("catalog.All");
            return ImmutableList.Create(First, Second);
        }
        if (method.Name == "GetEventTypeFor" && args is [Type type] && type == typeof(SelectedEvent))
        {
            Calls.Count("catalog.GetEventTypeFor");
            return Second;
        }
        throw Calls.Refuse("catalog." + method.Name);
    });
}
public sealed record SelectedEvent;

public sealed class RecordingInvoker : CallInvoker
{
    public List<object> Requests { get; } = [];
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
    {
        if (method.FullName != "/Cratis.Chronicle.Contracts.Observation.Webhooks.Webhooks/AddWebhooks")
            throw Calls.Refuse("rpc.unexpectedUnary");
        Calls.Count("rpc.AddWebhooks");
        var context = new RecordingSerializationContext();
        method.RequestMarshaller.ContextualSerializer(request, context);
        Calls.Count("marshaller.Serialize");
        var bytes = context.Bytes ?? throw new InvalidOperationException();
        var decoded = method.RequestMarshaller.ContextualDeserializer(new RecordingDeserializationContext(bytes));
        Calls.Count("marshaller.Deserialize");
        Requests.Add(new { method = method.FullName, requestType = typeof(TRequest).FullName, rawJSON = JsonSerializer.Serialize(request), bytesBase64 = Convert.ToBase64String(bytes), decodedJSON = JsonSerializer.Serialize(decoded) });
        // Fixed local default response only: not kernel acceptance or transport evidence.
        var response = Activator.CreateInstance<TResponse>();
        return new(Task.FromResult(response), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new Metadata(), () => { Calls.Count("rpc.Dispose"); });
    }
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw Calls.Refuse("rpc.BlockingUnary");
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw Calls.Refuse("rpc.ClientStreaming");
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw Calls.Refuse("rpc.DuplexStreaming");
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw Calls.Refuse("rpc.ServerStreaming");
}
public sealed class RecordingSerializationContext : SerializationContext
{
    readonly System.Buffers.ArrayBufferWriter<byte> _buffer = new();
    public byte[]? Bytes { get; private set; }
    public override void Complete(byte[] payload) => Bytes = payload;
    public override System.Buffers.IBufferWriter<byte> GetBufferWriter() => _buffer;
    public override void SetPayloadLength(int payloadLength) { }
    public override void Complete() => Bytes = _buffer.WrittenSpan.ToArray();
}
public sealed class RecordingDeserializationContext(byte[] bytes) : DeserializationContext
{
    public override int PayloadLength => bytes.Length;
    public override byte[] PayloadAsNewBuffer() => (byte[])bytes.Clone();
    public override System.Buffers.ReadOnlySequence<byte> PayloadAsReadOnlySequence() => new(bytes);
}
