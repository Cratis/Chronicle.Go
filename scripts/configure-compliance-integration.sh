#!/usr/bin/env bash
# Copyright (c) Cratis. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

# Only for a fresh, disposable 19.29.4-development test container. Never use on
# an existing store: losing this ephemeral certificate loses encrypted data.
set -euo pipefail
container=${1:?Pass the disposable integration container name or ID}

docker exec "$container" sh -ec '
    test ! -e /app/appsettings.Production.json
    test -z "$DOTNET_ENVIRONMENT"
    test -z "$ASPNETCORE_ENVIRONMENT"
    umask 077
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -subj /CN=chronicle-go-disposable-test \
        -keyout /tmp/compliance-test.key -out /tmp/compliance-test.crt >/dev/null 2>&1
    openssl pkcs12 -export -out /tmp/compliance-test.pfx \
        -inkey /tmp/compliance-test.key -in /tmp/compliance-test.crt -passout pass:
    printf "%s\n" '\''{"Cratis":{"Chronicle":{"EncryptionCertificate":{"CertificatePath":"/tmp/compliance-test.pfx","CertificatePassword":""}}}}'\'' > /app/appsettings.Production.json
'
docker restart "$container"
