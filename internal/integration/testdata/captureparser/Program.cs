// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json;
using Cratis.Screenplay;
using Cratis.Screenplay.Syntax.Captures;

// This checks actual Go-rendered declarations, not a second implementation of
// CaptureParser's regexes. Diagnostics deliberately omit declaration contents.
var cases = JsonSerializer.Deserialize<Case[]>(Console.In.ReadToEnd(), new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;
if (cases.Length == 0) throw new Exception("No capture parser cases supplied");
var compiler = new ScreenplayCompiler();
foreach (var (test, index) in cases.Select((test, index) => (test, index)))
{
    var result = compiler.CompileCapture(test.Declaration);
    Require(result.Success && result.Value is not null, "compilation");
    var capture = result.Value!;
    Require(capture.Name == test.Name, "capture name");
    var appends = capture.Appends.ToArray();
    Require(appends.Length == test.Conditions.Length, "append count");
    foreach (var (expected, appendIndex) in test.Conditions.Select((expected, appendIndex) => (expected, appendIndex)))
    {
        var append = appends[appendIndex];
        Require(append.Event == test.Event, "event identity");
        Require(append.When is not null, "condition presence");
        var when = append.When!;
        Require(when.Kind.ToString() == expected.Kind && when.Properties.SequenceEqual(expected.Properties), "condition kind/properties");
        Require(when.FromValue == expected.From && when.ToValue == expected.To, "transition literal round trip");
    }
    var translations = capture.Map.OfType<CaptureMapEntrySyntax>().SelectMany(entry => entry.Translations).ToArray();
    Require(translations.Length == test.Translations.Length, "translation count");
    foreach (var (expected, translationIndex) in test.Translations.Select((expected, translationIndex) => (expected, translationIndex)))
    {
        var actual = translations[translationIndex];
        Require(actual.From == expected.From && actual.To == expected.To, "translation literal round trip");
    }
    Require(capture.Map.OfType<CaptureSplitSyntax>().Select(split => split.Separator).SequenceEqual(test.Separators), "split literal round trip");
    void Require(bool condition, string subject)
    {
        if (!condition) throw new Exception($"Capture case {index}: {subject} differs");
    }
}
// .NET regexes match UTF-16 code units rather than Go runes: a supplementary
// letter is not a \w token. These negative controls also guard the exact
// grammar assumptions used by the Go construct-specific validators.
var header = "capture Capture\n  source api\n    api Service\n  key id\n";
var invalid = new[]
{
    "capture Orders.Capture\n  key id\n",
    header + "  append Orders.Changed\n    when added\n",
    header + "  append changed\n    when added\n",
    header + "  append Changed𐐀\n    when added\n",
    header + "  map\n    status = status translate\n      \"draft\" => \"open\"\n",
    header + "  map\n    status = status translate\n      \"draft\" => 𐐀\n"
};
foreach (var source in invalid)
{
    if (compiler.CompileCapture(source).Success) throw new Exception("Pinned parser accepted a negative grammar control");
}
Console.WriteLine($"Parsed {cases.Length} Go declarations and rejected {invalid.Length} negative controls with Screenplay 4.16.0; all identities, condition kinds and literal round trips match");

record Case(string Declaration, string Name, string Event, Condition[] Conditions, Translation[] Translations, string[] Separators);
record Condition(string Kind, string[] Properties, string? From = null, string? To = null);
record Translation(string From, string To);
