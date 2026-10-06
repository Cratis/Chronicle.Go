// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Cratis.Chronicle;
using Cratis.Chronicle.Compliance;
using Cratis.Chronicle.Events;
using Cratis.Serialization;

internal static class Isolation
{
    const BindingFlags PrivateInstance = BindingFlags.Instance | BindingFlags.NonPublic;

    public static JsonSerializerOptions ClientOptions(INamingPolicy naming)
    {
        // Execute the actual packaged initializer, not a copied converter list.
        // No ChronicleClient constructor, type-universe discovery, connection or disposal.
        var client = (ChronicleClient)RuntimeHelpers.GetUninitializedObject(typeof(ChronicleClient));
        var options = new ChronicleOptions { AutoDiscoverAndRegister = false };
        typeof(ChronicleClient).GetField("<Options>k__BackingField", PrivateInstance)!.SetValue(client, options);
        typeof(ChronicleClient).GetField("_namingPolicy", PrivateInstance)!.SetValue(client, naming);
        typeof(ChronicleClient).GetMethod("InitializeJsonSerializationOptions", PrivateInstance)!.Invoke(client, null);
        return options.JsonSerializerOptions;
    }

    public static EventSerializer Serializer(JsonSerializerOptions options, IDerivedTypes derived)
    {
        // The activator interface is internal in some packaged targets. Invoke the
        // exact constructor; its unused activator is null because providers are empty.
        // Registry calls fail the entire capture, including if caught per observation.
        var ctor = typeof(EventSerializer).GetConstructors().Single();
        return (EventSerializer)ctor.Invoke([new EmptyArtifacts(), null,
            DispatchProxy.Create<IEventTypes, ForbiddenRegistry>(), options, derived]);
    }

    public static JsonSerializerOptions SerializerOptions(EventSerializer serializer) =>
        (JsonSerializerOptions)typeof(EventSerializer).GetField("_serializerOptions", PrivateInstance)!.GetValue(serializer)!;

    public static JsonSerializerOptions SchemaOptions(object generator) =>
        (JsonSerializerOptions)generator.GetType().GetField("_serializerOptions", PrivateInstance)!.GetValue(generator)!;
}

public class ForbiddenRegistry : DispatchProxy
{
    public static int Calls { get; private set; }
    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
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

public sealed class EmptyArtifacts : IClientArtifactsProvider
{
    public IEnumerable<Type> EventTypes => [];
    public IEnumerable<Type> Projections => [];
    public IEnumerable<Type> ModelBoundProjections => [];
    public IEnumerable<Type> Reactors => [];
    public IEnumerable<Type> ReadModelReactors => [];
    public IEnumerable<Type> Reducers => [];
    public IEnumerable<Type> ReactorMiddlewares => [];
    public IEnumerable<Type> ComplianceForTypesProviders => [];
    public IEnumerable<Type> ComplianceForPropertiesProviders => [];
    public IEnumerable<Type> AdditionalEventInformationProviders => [];
    public IEnumerable<Type> ConstraintTypes => [];
    public IEnumerable<Type> UniqueConstraints => [];
    public IEnumerable<Type> UniqueEventTypeConstraints => [];
    public IEnumerable<Type> RemoveConstraintEventTypes => [];
    public IEnumerable<Type> EventTypeMigrators => [];
    public IEnumerable<Type> EventSeeders => [];
}
