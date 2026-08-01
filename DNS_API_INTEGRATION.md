# Integração com API de DNS do DTunnel

## Visão Geral

A API de DNS do DTunnel permite criar hostnames dinâmicos que apontam para o servidor XHTTP, eliminando a necessidade de configurar manualmente o IP no app cliente.

**Endpoint:** `https://dns.dtunnel.com.br/api/v1/dns/create`

---

## Fluxo de Funcionamento

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Servidor XHTTP inicia                                    │
│    - Obtém IP público                                       │
│    - Chama API de DNS para criar hostname                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. API DTunnel cria registro DNS                            │
│    - hostname.dtunnel.com.br → IP_SERVIDOR                  │
│    - TTL: 60 segundos (dinâmico)                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. App DTunnel conecta                                      │
│    - Usa hostname em vez de IP                              │
│    - Conexão automática sem reconfiguração                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Especificação da API

### Criar Hostname Dinâmico

**URL:** `POST https://dns.dtunnel.com.br/api/v1/dns/create`

**Headers:**
```http
Content-Type: application/json
Authorization: Bearer {token_opcional}
```

**Body:**
```json
{
  "hostname": "meu-servidor",
  "ip": "203.0.113.42",
  "ttl": 60,
  "server_id": "965866"
}
```

**Parâmetros:**

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `hostname` | string | Sim | Nome do host (sem domínio) |
| `ip` | string | Sim | Endereço IP público do servidor |
| `ttl` | integer | Não | Time-to-Live em segundos (padrão: 60) |
| `server_id` | string | Não | ID do servidor para rastreamento |

**Resposta de Sucesso (200):**
```json
{
  "success": true,
  "hostname": "meu-servidor.dtunnel.com.br",
  "ip": "203.0.113.42",
  "ttl": 60,
  "created_at": "2026-07-29T17:30:00Z",
  "expires_at": "2026-07-29T17:31:00Z"
}
```

**Resposta de Erro (400/401/500):**
```json
{
  "success": false,
  "error": "Invalid hostname format",
  "code": "INVALID_HOSTNAME"
}
```

---

## Implementação no SCrpro2

### 1. Estrutura de Dados

```rust
#[derive(Serialize, Deserialize)]
struct DnsCreateRequest {
    hostname: String,
    ip: String,
    ttl: Option<u32>,
    server_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct DnsCreateResponse {
    success: bool,
    hostname: String,
    ip: String,
    ttl: u32,
    created_at: String,
    expires_at: String,
}
```

### 2. Função de Criação de DNS

```rust
async fn create_dns_hostname(ip: &str, server_id: &str) -> Result<String, XhttpError> {
    let client = reqwest::Client::new();
    
    let hostname = format!("srv-{}", server_id);
    
    let req = DnsCreateRequest {
        hostname: hostname.clone(),
        ip: ip.to_string(),
        ttl: Some(60),
        server_id: Some(server_id.to_string()),
    };
    
    let resp = client
        .post("https://dns.dtunnel.com.br/api/v1/dns/create")
        .json(&req)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    
    if resp.status().is_success() {
        let data: DnsCreateResponse = resp.json().await?;
        Ok(data.hostname)
    } else {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "DNS creation failed"
        )))
    }
}
```

### 3. Integração no Main

```rust
#[tokio::main]
async fn main() -> Result<(), XhttpError> {
    let port = get_port();
    let status = get_status();
    let ssh_port = get_ssh_port();
    let server_id = get_server_id();
    
    // Obter IP público
    let public_ip = get_public_ip().await.unwrap_or_else(|_| "127.0.0.1".to_string());
    
    // Criar hostname dinâmico
    if let Ok(hostname) = create_dns_hostname(&public_ip, &server_id).await {
        println!("[DNS] Hostname criado: {}", hostname);
        println!("[INFO] Configure no app: {}", hostname);
    }
    
    // Iniciar servidor...
}
```

---

## Obter IP Público

```rust
async fn get_public_ip() -> Result<String, XhttpError> {
    let client = reqwest::Client::new();
    
    // Tenta múltiplos serviços
    let services = vec![
        "https://api.ipify.org",
        "https://icanhazip.com",
        "https://ident.me",
    ];
    
    for url in services {
        if let Ok(resp) = client.get(url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(text) = resp.text().await {
                let ip = text.trim();
                if is_valid_ip(ip) {
                    return Ok(ip.to_string());
                }
            }
        }
    }
    
    Err(Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        "Could not determine public IP"
    )))
}

fn is_valid_ip(ip: &str) -> bool {
    ip.parse::<std::net::IpAddr>().is_ok()
}
```

---

## Tratamento de Erros

### Erros Comuns

| Erro | Causa | Solução |
|------|-------|---------|
| `INVALID_HOSTNAME` | Formato inválido | Use apenas letras, números e hífens |
| `HOSTNAME_TAKEN` | Hostname já existe | Adicione timestamp ou ID único |
| `INVALID_IP` | IP mal formatado | Valide o formato antes de enviar |
| `RATE_LIMIT` | Muitas requisições | Implemente backoff exponencial |
| `NETWORK_ERROR` | Sem conexão | Tente novamente com timeout |

### Retry Logic

```rust
async fn create_dns_with_retry(ip: &str, server_id: &str) -> Result<String, XhttpError> {
    let mut attempts = 0;
    let max_attempts = 3;
    
    loop {
        match create_dns_hostname(ip, server_id).await {
            Ok(hostname) => return Ok(hostname),
            Err(e) if attempts < max_attempts => {
                attempts += 1;
                let delay = Duration::from_secs(2_u64.pow(attempts as u32));
                println!("[DNS] Retry {} em {:?}...", attempts, delay);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

---

## Monitoramento e Logs

### Estrutura de Logs

```rust
println!("[DNS] Iniciando criação de hostname...");
println!("[DNS] IP Público: {}", public_ip);
println!("[DNS] Server ID: {}", server_id);
println!("[DNS] Hostname: {}", hostname);
println!("[DNS] TTL: 60 segundos");
println!("[DNS] Endpoint: https://dns.dtunnel.com.br/api/v1/dns/create");
```

### Exemplo de Saída

```
[SDProxy] xHTTP v4.0.0 (DTunnel API + Sequence Fixed)
[xHTTP] Porta: 443 | SSH Backend: 127.0.0.1:22
[DNS] Iniciando criação de hostname...
[DNS] IP Público: 203.0.113.42
[DNS] Server ID: 965866
[DNS] Hostname: srv-965866.dtunnel.com.br
[DNS] TTL: 60 segundos
[INFO] Configure no app: srv-965866.dtunnel.com.br
[xHTTP] Aguardando conexões...
```

---

## Configuração no App DTunnel

Após criar o hostname, configure no app:

1. **Server:** `srv-965866.dtunnel.com.br`
2. **Port:** `443`
3. **Protocol:** `XHTTP`
4. **TLS:** `Habilitado`
5. **SNI:** `srv-965866.dtunnel.com.br`
6. **Path:** `/ssh`

---

## Segurança

### Boas Práticas

1. **Validar Entrada**
   ```rust
   fn validate_hostname(hostname: &str) -> bool {
       hostname.len() <= 63 &&
       hostname.chars().all(|c| c.is_alphanumeric() || c == '-') &&
       !hostname.starts_with('-') &&
       !hostname.ends_with('-')
   }
   ```

2. **Rate Limiting**
   - Máximo 1 criação por IP por minuto
   - Máximo 10 criações por servidor por hora

3. **Autenticação**
   - Token opcional no header `Authorization`
   - Validar origem da requisição

4. **HTTPS Obrigatório**
   - Todas as requisições devem usar HTTPS
   - Validar certificado SSL

---

## Testes

### Teste Manual

```bash
curl -X POST https://dns.dtunnel.com.br/api/v1/dns/create \
  -H "Content-Type: application/json" \
  -d '{
    "hostname": "teste-965866",
    "ip": "203.0.113.42",
    "ttl": 60,
    "server_id": "965866"
  }'
```

### Teste de Resolução

```bash
nslookup teste-965866.dtunnel.com.br
dig teste-965866.dtunnel.com.br
```

---

## Roadmap Futuro

- [ ] Suporte a IPv6
- [ ] Wildcard DNS (`*.dtunnel.com.br`)
- [ ] Atualização dinâmica de IP
- [ ] Deletar hostname expirado
- [ ] Dashboard de hostnames ativos
- [ ] Webhook de notificação

---

## Referências

- [DTunnel Official](https://dtunnel.com.br)
- [RFC 1035 - DNS Protocol](https://tools.ietf.org/html/rfc1035)
- [Reqwest Documentation](https://docs.rs/reqwest/)
- [Tokio Async Runtime](https://tokio.rs)

---

**Versão:** 1.0  
**Data:** 29 de Julho de 2026  
**Status:** Documentação Técnica
