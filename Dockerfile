# syntax=docker/dockerfile:1.7

# Stage 1 — chef: install cargo-chef on top of the rust toolchain image.
FROM rust:1.95-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

# Stage 2 — planner: compute the dependency-only recipe from Cargo manifests.
# Only Cargo.toml / Cargo.lock changes invalidate this layer, so source-only
# edits skip straight to the cached `cook` layer below.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3 — cook: build *only* the dependency graph using the recipe.
# This layer is reused as long as the recipe hash is unchanged.
FROM chef AS cook
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --no-default-features \
    --features fuse,vendored-openssl --recipe-path recipe.json

# Stage 4 — build the actual binaries; deps come from the cooked cache.
FROM cook AS builder
COPY . .
RUN cargo build --release --no-default-features --features fuse,vendored-openssl \
    --bin hf-mount-fuse --bin hf-mount-fuse-sidecar

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libfuse3-3 ca-certificates && rm -rf /var/lib/apt/lists/*

# TRUST INTERNAL ACCELERATOR S3
COPY <<EOF /usr/local/share/ca-certificates/harica-geant.crt
-----BEGIN CERTIFICATE-----
MIIDtjCCAp4CCQDFm01lBlHbcDANBgkqhkiG9w0BAQsFADCBnDELMAkGA1UEBhMC
QVQxEDAOBgNVBAgMB0F1c3RyaWExEjAQBgNVBAcMCUxheGVuYnVyZzEOMAwGA1UE
CgwFSUlBU0ExDDAKBgNVBAsMA0lDVDEgMB4GA1UEAwwXY2VydGlmaWNhdGUuaWlh
c2EuYWMuYXQxJzAlBgkqhkiG9w0BCQEWGGljdC5oZWxwZGVza0BpaWFzYS5hYy5h
dDAeFw0yMzAzMjMxMzA3MDVaFw00MzEwMDQxMzA3MDVaMIGcMQswCQYDVQQGEwJB
VDEQMA4GA1UECAwHQXVzdHJpYTESMBAGA1UEBwwJTGF4ZW5idXJnMQ4wDAYDVQQK
DAVJSUFTQTEMMAoGA1UECwwDSUNUMSAwHgYDVQQDDBdjZXJ0aWZpY2F0ZS5paWFz
YS5hYy5hdDEnMCUGCSqGSIb3DQEJARYYaWN0LmhlbHBkZXNrQGlpYXNhLmFjLmF0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA08pA8TlPOhQ1rg2zBXgy
2ZOAPSB1GKsxuLhgqRh9MxBkfBKqqwbuvt2r/DFrqOccKY2njgKdwmxweqcp2T/H
hH756LOHiEZNvv6zBodpkYMF+VxSkepVTPIvNdHCFvy12c2uM4dL7pHhOqVBf6Ly
2wfmP/fj0mwJeRLx8wDvyMUkKf3kC6UTvT5AbK0LI6jeyLxJlzF6YQqGK6L52RS1
Pbnu4gIODHJHsNshg1QmBCQYI6v1L4FXgosNbksPf05wL2SB+DI/kktLP8qSXtkx
IV7WBPsilnu8R0md2wHL+WUNTwmukB2W6KlRqoqSgZJ3nNRaqnOq8HLU3FR0Fjn9
JwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAexeWy9rEH3x0SLK2D8VBgggIJv3iY
ZPeMMAotF9fop/+Tf4KrTs3tbs4mwDmg9dlxMNlAYvdOyC1mfSfg5qjCF71WRxY2
a+9sIb2rvmaQ5pEuO8i7RGgTOeHj5E7f8UoCwRnC+JUw52eOTjcCfw1QxoWGieiB
whrNbNhjI0xWDNxLb2VZ0rfFtO6lEFzVQbF6GIXq4QOjxtWRV/DQKX+S4aZgmniT
0vTP1bVoS1vHkidVAFZ9v82pCGZFXpjku/gjjmO4Yc/In/WeqiyKZ2HRzIO/ZcGk
nP6/j/YnBT9ayxJE5ku2OXNh/EiuNZRytdImcik6K4TePQjhvP4gXmK5
-----END CERTIFICATE-----
EOF

COPY <<EOF /usr/local/share/ca-certificates/backend.crt
-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUcNK7dtpLf7QnDMUK0isLDRlBe18wDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKbG9jYWxpcC1jYTAeFw0yNjA3MDEwMTExMjhaFw0zNjA2
MjgwMTExMjhaMBUxEzARBgNVBAMMCmxvY2FsaXAtY2EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCIaDGclU20NiTs0WE90cXyrEUtTCZgmrWnZcXNllCy
9EX+67DHpDA0QcdmQEclprl9sNeLanzEjf0TwM+WhMaawImHWBCYH1e4fm7+wPL5
Y8jObwYYBuXmt8P4M47vE+kKEMBLDxx2aitlpbRpAsqMoAsWD1XQ1HU34rDWCve+
7fmMAPo1t42+U947geaU9pZRje83iJO4mqs9kMH/QKd4z/93hAn2zXa/jmBRjpxk
187iFiQgJnyJWVwmxWqyb4p5zQDLwPZi/X72WYSu7+YQjrwQbW02N/MV9YMmgvi6
Lo08/VBRAeuJFI1GXqLDnnlg2O5FExcEPvYvDIlH0nflAgMBAAGjYzBhMB0GA1Ud
DgQWBBSkISilMdBxZW+e+oGL31kHfoxJwjAfBgNVHSMEGDAWgBSkISilMdBxZW+e
+oGL31kHfoxJwjAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBBjANBgkq
hkiG9w0BAQsFAAOCAQEABlKCf+RuMxTfan+08qvBKheozCHlKLsRYGV6pgElRFpS
e4YyZrXlXfnuZ4CDBA80K4Lq80+YWyvNuxHFOzvBx8cLuUkWwahufmqOFfuuYAFb
r7JV1Tkvz5qqjz53E4Np9nEj2ltorlpLpN/cNe7XzTjOEaQNcW92kzS5C7x3WCi9
Skj8B3FC30aNb+6pd5+WBut0o/z9NPs3npwLA/z7uWq4fFWgyHlNg/MTZvoYKLo1
musGdLIA8UTO4M0HjLBr2BpyxbDLUiSrDO0XXsQU/W3Ogs/d+d8JcR5XLEh5AmEc
ISKGDrVKkpB4nJWwLWW2L97AI1YoIkaiz6v/Huxk3w==
-----END CERTIFICATE-----
EOF

COPY <<EOF /usr/local/share/ca-certificates/minio.crt
-----BEGIN CERTIFICATE-----
MIIDITCCAgmgAwIBAgIUTt8K6LXKl5gfEhCRCfePSv2pHY8wDQYJKoZIhvcNAQEL
BQAwGDEWMBQGA1UEAwwNbG9jYWxpcC1zMy1jYTAeFw0yNjA3MDEwMTM2NDdaFw0z
NjA2MjgwMTM2NDdaMBgxFjAUBgNVBAMMDWxvY2FsaXAtczMtY2EwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQDj6J6R1DL+U2yd+whF1RhMSh0zFHTkj7Hd
/qfGk8VIW/PXVxC72QCMT8nAIe7quA00fNAGs1og1H6fvqV6JwW8lyv0CJyZKP4i
hMwnPfXrNZzWontVyWd4ivHqq2ocv6V8Cf7BJLeyqzKxMROg9/bP84iCin3KUp2z
YCNJJpyQMlo32TfiGob2szxMBWebhbxBVvAR9/wtW9AulvkLLhZJuUJ3Ou7AzwdQ
BGeRVEaBVZtYeedjuhAbYxEd3RqDk9n8B96AJb0sl4NzrM7onYWen2J83MTjxShc
xW9vJrFU5gvWPKujHLsh/jGS3DK7pXTgH0WcBHcupEBM7zMt7t8LAgMBAAGjYzBh
MB0GA1UdDgQWBBTHnWQB33tGEKQYsOvit1CD/ZPc4jAfBgNVHSMEGDAWgBTHnWQB
33tGEKQYsOvit1CD/ZPc4jAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIB
BjANBgkqhkiG9w0BAQsFAAOCAQEAcwpghPOfJbhGt1f7rTzppDS98uF6oPoPtLCa
znmGXvsA0TYtJK+JoqXweLniN31sgVejh3o7gprNUYm7trCj7u+tvvLbqKau3yf7
aIRzHqbXCuUKRnSz9jAtCeMlLQ1N1eumxl5kY2Y2HN9osimuUmElyQzOKG3nm4Om
AKm5DIHmAeT8nHcZ5ZMW7aLIhKh4onreOaERf6jG2G3xyTVAK3WAmE5zAm+AVTj4
QiCteefuVdBYglaICfAUXzL0u3AWagSspbCVDFZN6e5xeRPhkawO5QqRwaqQ6V68
lZ+Pbp8nH0ttPG2Jep2k2wfDukKJgF9Oz4NE07MdaGBNabhALA==
-----END CERTIFICATE-----
EOF

RUN update-ca-certificates && \
    cat /usr/local/share/ca-certificates/harica-geant.crt \
    /usr/local/share/ca-certificates/backend.crt \
    /usr/local/share/ca-certificates/minio.crt \
    >> /etc/ssl/certs/ca-certificates.crt

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs
# END TRUST INTERNAL ACCELERATOR S3

# `upgrade` pulls the Debian security updates the base image is behind on at build
# time (2026-09-28: openssl/libssl3 3.0.20-1~deb12u2 shipped while 3.0.22-1~deb12u1
# fixed CVE-2026-75803), so a rebuild is enough to clear an OS-package CVE. The layer
# is cached in the registry: APT_REFRESH is set to the ISO week by the build, so the
# cache expires weekly instead of at the next base-image bump. BuildKit only misses
# the cache where the ARG is used, hence the echo.
ARG APT_REFRESH=unset
RUN echo "apt refresh: ${APT_REFRESH}" && apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends libfuse3-3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hf-mount-fuse /usr/local/bin/
COPY --from=builder /build/target/release/hf-mount-fuse-sidecar /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/hf-mount-fuse"]
