# Security policy

## Report a vulnerability privately

Do not report a suspected vulnerability in a public GitHub issue, pull request, or discussion.

Email [oss@cratis.io](mailto:oss@cratis.io?subject=Security%3A) with `Security:` at the start of the subject. Include:

- the repository (`Cratis/Chronicle.Go`), Go version, OS/architecture, and exact commit or module version;
- what you observed and what you expected;
- minimal reproduction steps and the conditions needed to reach the behavior;
- the potential impact as you understand it; and
- a safe way to contact you for follow-up.

Leave credentials, personal data, customer data, and production logs out of the first message. Ask for a private transfer method if more evidence is needed.

## Supported versions

This project is in early development, with no stable release or tagged Go module yet. Reports against the current `main` branch are welcome. Once experimental v0.x releases exist, include the affected module version; no long-term support commitment is made.

## Scope

This policy provides a private reporting route. It is not a response-time commitment, service-level agreement, warranty, or bounty. See the [Cratis security policy](https://github.com/Cratis/.github/blob/main/SECURITY.md) for the organization-wide terms.

Vulnerabilities in the Chronicle kernel or other Chronicle clients belong to their respective repositories, including [Chronicle](https://github.com/Cratis/Chronicle). Use the same private route and name the affected repository.
