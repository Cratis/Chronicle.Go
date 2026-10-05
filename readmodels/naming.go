// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// English inflection rules adapted from Humanizer (MIT); see HUMANIZER-LICENSE.

package readmodels

import (
	"regexp"
	"strings"
	"unicode"
)

type pluralRule struct {
	match       *regexp.Regexp
	replacement string
}

// Only the known-singular pluralization path is used by C# DefaultNamingPolicy.
// Go type names cannot contain the whitespace used by Humanizer compound phrases.
var pluralRules = func() []pluralRule {
	rules := [][2]string{
		{"$", "s"}, {"s$", "s"}, {"(ax|test)is$", "${1}es"},
		{"(octop|vir|alumn|fung|cact|foc|hippopotam|radi|stimul|syllab|nucle)us$", "${1}i"},
		{"(alias|bias|iris|status|campus|apparatus|virus|walrus|trellis)$", "${1}es"},
		{"(buffal|tomat|volcan|ech|embarg|her|mosquit|potat|torped|vet)o$", "${1}oes"},
		{"([dti])um$", "${1}a"}, {"sis$", "ses"},
		{"(cal|dwar|el|hal|hoo|lea|loa|scar|sel|shel|thie|tur|whar|wol)f$", "${1}ves"},
		{"(kni|li|wi)fe$", "${1}ves"}, {"(hive)$", "${1}s"}, {"([^aeiouy]|qu)y$", "${1}ies"},
		{"(x|ch|ss|sh)$", "${1}es"}, {"(matr|vert|ind|d)(ix|ex)$", "${1}ices"},
		{"(^[m|l])ouse$", "${1}ice"}, {"^(ox)$", "${1}en"}, {"(quiz)$", "${1}zes"},
		{"(buz|blit|walt)z$", "${1}zes"}, {"(alumn|alg|larv|vertebr)a$", "${1}ae"}, {"(criteri|phenomen)on$", "${1}a"},
	}
	for _, pair := range [][2]string{{"person", "people"}, {"man", "men"}, {"human", "humans"}, {"child", "children"}, {"sex", "sexes"}, {"glove", "gloves"}, {"move", "moves"}, {"goose", "geese"}, {"wave", "waves"}, {"foot", "feet"}, {"tooth", "teeth"}, {"curriculum", "curricula"}, {"database", "databases"}, {"zombie", "zombies"}, {"personnel", "personnel"}, {"cache", "caches"}} {
		rules = append(rules, [2]string{"(" + pair[0][:1] + ")" + pair[0][1:] + "$", "${1}" + pair[1][1:]})
	}
	for _, pair := range [][2]string{{"olive", "olives"}, {"ex", "exes"}, {"is", "are"}, {"was", "were"}, {"that", "those"}, {"this", "these"}, {"bus", "buses"}, {"die", "dice"}, {"tie", "ties"}} {
		rules = append(rules, [2]string{"^" + pair[0] + "$", pair[1]})
	}
	for _, pair := range [][2]string{{"lens", "lenses"}, {"clove", "cloves"}, {"valve", "valves"}, {"explosive", "explosives"}} {
		rules = append(rules, [2]string{"(" + pair[0][:1] + ")" + pair[0][1:] + "$", "${1}" + pair[1][1:]})
	}
	compiled := make([]pluralRule, len(rules))
	for i, rule := range rules {
		compiled[i] = pluralRule{regexp.MustCompile("(?i)" + rule[0]), rule[1]}
	}
	return compiled
}()

func pluralize(word string) string {
	lower := strings.ToLower(word)
	if strings.Trim(lower, "s") == "" {
		return word[:1] + "s"
	}
	for _, uncountable := range strings.Fields("staff training software equipment information corn milk rice money species series fish sheep deer aircraft oz tsp tbsp ml l water waters semen sperm bison grass hair mud elk luggage moose offspring salmon shrimp someone swine trout tuna corps scissors means mail pliers sheers clothes apparatus chassis debris metadata") {
		if lower == uncountable {
			return word
		}
	}
	for i := len(pluralRules) - 1; i >= 0; i-- {
		rule := pluralRules[i]
		if !rule.match.MatchString(word) {
			continue
		}
		result := rule.match.ReplaceAllString(word, rule.replacement)
		hasUpper, hasLower := false, false
		for _, r := range word {
			hasUpper = hasUpper || unicode.IsUpper(r)
			hasLower = hasLower || unicode.IsLower(r)
		}
		if len([]rune(word)) > 1 && hasUpper && !hasLower {
			return strings.ToUpper(result)
		}
		original, replacement := []rune(word), []rune(result)
		if unicode.IsUpper(original[0]) {
			replacement[0] = unicode.ToUpper(replacement[0])
		}
		return string(replacement)
	}
	return word
}
