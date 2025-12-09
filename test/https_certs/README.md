# Certificate generation

Certificate and key in this folder is generated using the following command:
```bash
openssl req -config cert.conf  -keyout key.pem -out cert.pem  -x509 -days 365
```
