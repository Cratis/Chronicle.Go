// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
public class BinaryNested { public byte[] Inner { get; set; } = []; }
public class BinaryEvent
{
    public byte[] Payload { get; set; } = [];
    public byte[]? Optional { get; set; }
    public List<byte[]> Chunks { get; set; } = [];
    public BinaryNested Nested { get; set; } = new();
}
public class BinaryModel
{
    public byte[] Payload { get; set; } = [];
    public byte[]? Optional { get; set; }
    public List<byte[]> Chunks { get; set; } = [];
    public BinaryNested Nested { get; set; } = new();
}
public record BinaryRecord(byte[] Payload, byte[]? Optional = null);
public class BinaryMapControl { public Dictionary<string, byte[]> ByKey { get; set; } = new(); }
