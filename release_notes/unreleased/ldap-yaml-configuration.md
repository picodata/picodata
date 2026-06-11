## feat/config

##### LDAP is now configurable through yaml

LDAP authentication can now be configured with `picodata.yaml` configuration file. This also adds ability to use alternative (non-system-wide) trusted root CAs file when connecting to LDAP server via TLS.

For now, previous configuration method through environment variables (`TT_LDAP_URL`, `TT_LDAP_DN_FMT`, `TT_LDAP_ENABLE_TLS`) is still supported, but considered obsolete. A warning will be printed on start.

Here's an example YAML configuration snippet for configuring LDAP:

```yaml
instance:
  ldap:
    enabled: true                             # `false` by default. LDAP authentication will fail if set to `false`.
    connect: 127.0.0.1:1337                   # Address of the LDAP server to connect to.
    dn_format: "cn=$USER,dc=example,dc=org"   # Defines conversion of picodata username to an LDAP Distinguished Name (DN).
                                              # Must have exactly one occurrence of `$USER` in it.
    tls:
      enabled: true                           # `false` by default. If `true`, TLS will be used to connect to the server
      method: start_tls                       # `implicit` by default. Accepted values are `implicit` and `start_tls`.
                                              # `implicit` means using `ldaps`. `start_tls` means using LDAP over TLS (StartTLS).
      ca_file: /etc/picodata/ldap-root-ca.crt # Path to a file containing alternative trusted root CA certificates, formatted as PEM.
                                              # System trusted certificate store will be ignored, and those certificates will be used instead.
```

###### Migration from legacy environment variables

You need to convert the

1. The old value of `TT_LDAP_DN_FMT` should be put into `instance.ldap.dn_format`. The format strings are fully compatible.

2. For `TT_LDAP_URL` and `TT_LDAP_ENABLE_TLS`:

| `TT_LDAP_URL`               | `TT_LDAP_ENABLE_TLS` | Action                                                                                                                                                |
|-----------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ldap://[hostname]:[port]`  | not set              | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Leave other options as default values.                                                        |
| `ldap://[hostname]:[port]`  | `true`               | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Set `instance.ldap.tls.enabled` to `true`.<br/>Set `instance.ldap.tls.method` to `start_tls`. |
| `ldaps://[hostname]:[port]` | not set              | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Set `instance.ldap.tls.enabled` to `true`.<br/>Set `instance.ldap.tls.method` to `implicit`.  |
| `ldaps://[hostname]:[port]` | `true`               | This is an invalid configuration, since it requests use of StartTLS while also using LDAPS (implicit TLS). It is unrepresentable with the new config. |
