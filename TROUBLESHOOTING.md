# Guia de Troubleshooting - XHTTP e DTunnel

## Problema: "Conectando" Infinito

### Sintomas
- App HS2 NET fica preso em "Conectando"
- Nenhuma mensagem de erro é exibida
- Timeout após alguns minutos

### Causas Possíveis

1. **Reordenação de Pacotes Não Funcionando**
   - Pacotes chegam fora de ordem
   - Buffer não reordena corretamente
   - Solução: Verificar se `BTreeMap` está sendo usado

2. **Sessão XHTTP Não Criada**
   - GET não recebe resposta correta
   - Path parsing incorreto
   - Solução: Verificar logs de path parsing

3. **TLS Handshake Falhando**
   - Cliente envia ClientHello sem signature_algorithms
   - rustls rejeita a conexão
   - Solução: Usar native-tls em vez de rustls ✓ (já implementado)

### Solução

```bash
# 1. Verificar se o servidor está rodando
ss -tlnp | grep 443

# 2. Testar conexão TLS
openssl s_client -connect localhost:443

# 3. Verificar logs
journalctl -u proxy-443.service -f

# 4. Testar GET
curl -i https://localhost:443/ssh/test-session

# 5. Testar POST
curl -i -X POST https://localhost:443/ssh/test-session/0 \
  -d "test data"
```

---

## Problema: "There was a problem while connecting to bu...app:433"

### Sintomas
- Erro exato: "There was a problem while connecting to bu...app:433"
- Conexão é estabelecida mas falha logo depois
- XHTTP download started aparece nos logs

### Causa Raiz
O domínio mascarado (`bu...app`) é um proxy intermediário que valida a conexão. Se a resposta XHTTP não está correta, o proxy rejeita.

### Solução

1. **Verificar Headers HTTP**
   ```rust
   // Deve incluir estes headers
   "HTTP/1.1 200 OK\r\n\
    Connection: keep-alive\r\n\
    Content-Type: application/octet-stream\r\n\
    Transfer-Encoding: chunked\r\n\
    X-Session-ID: {session_id}\r\n\
    X-Status: @SDProxy\r\n\r\n"
   ```

2. **Verificar Chunked Encoding**
   ```rust
   // Formato correto de chunk
   "{:x}\r\n{data}\r\n"
   
   // Exemplo: 5 bytes
   "5\r\nhello\r\n"
   
   // Chunk final
   "0\r\n\r\n"
   ```

3. **Verificar Sequenciamento**
   ```rust
   // POST deve incluir sequence number
   POST /ssh/{session_id}/0
   POST /ssh/{session_id}/1
   POST /ssh/{session_id}/2
   ```

---

## Problema: "XHTTP download started" Aparece mas Sem Dados

### Sintomas
- Mensagem "XHTTP download started" é recebida
- Nenhum dado SSH é transmitido
- Conexão fica presa

### Causa
O marcador "XHTTP download started" é enviado, mas o GET não está recebendo dados do SSH backend.

### Solução

1. **Verificar Conexão SSH**
   ```bash
   # Testar SSH backend
   ssh -v localhost -p 22
   
   # Verificar se SSH está rodando
   systemctl status ssh
   ```

2. **Verificar Reordenação de Pacotes**
   ```rust
   // Adicionar logs para debug
   println!("[XHTTP] Recebido POST seq={} com {} bytes", seq, data.len());
   println!("[XHTTP] Buffer contém seqs: {:?}", buffer.keys());
   ```

3. **Verificar Timeout**
   ```rust
   // Aumentar timeout se necessário
   timeout(Duration::from_secs(600), sr.read(&mut b)).await
   ```

---

## Problema: CheckUser Retorna Erro 404

### Sintomas
- Endpoint `/checkUser` não encontrado
- Erro 404 page not found
- App não consegue validar usuário

### Causa
Path parsing não reconhece `/checkUser` como endpoint especial.

### Solução

```rust
// Verificar se a função handle_check_user está sendo chamada
if path.contains("/checkUser") || path.contains("/user") {
    return handle_check_user(&mut stream, &path).await;
}

// Adicionar logs
println!("[CheckUser] Path: {}", path);
println!("[CheckUser] Contém /checkUser: {}", path.contains("/checkUser"));
```

---

## Problema: API DTunnel Não Responde

### Sintomas
- Timeout ao chamar `https://api.dtunnel.com.br/api/checkUser`
- CheckUser retorna JSON dummy
- App aceita qualquer usuário

### Causa
API do DTunnel pode estar offline ou bloqueando requisições.

### Solução

```bash
# 1. Testar conectividade
curl -i https://api.dtunnel.com.br/api/checkUser?user=965866

# 2. Verificar DNS
nslookup api.dtunnel.com.br

# 3. Verificar firewall
telnet api.dtunnel.com.br 443

# 4. Usar timeout apropriado
timeout(Duration::from_secs(5), client.get(&api_url).send()).await
```

---

## Problema: DNS Hostname Não Resolve

### Sintomas
- `srv-965866.dtunnel.com.br` não resolve
- `nslookup` retorna NXDOMAIN
- App não consegue conectar usando hostname

### Causa
API de DNS não criou o registro ou TTL expirou.

### Solução

```bash
# 1. Verificar resolução
nslookup srv-965866.dtunnel.com.br
dig srv-965866.dtunnel.com.br

# 2. Forçar refresh de DNS
systemctl restart systemd-resolved

# 3. Testar com IP direto
curl -i https://203.0.113.42:443/checkUser?user=965866

# 4. Verificar logs de criação de DNS
grep "DNS" /var/log/sdproxy.log
```

---

## Problema: Sessão XHTTP Expira Rapidamente

### Sintomas
- Conexão cai após alguns segundos
- Timeout de 600 segundos não é respeitado
- Sessão é removida prematuramente

### Causa
Timeout ou limpeza de sessão prematura.

### Solução

```rust
// Aumentar timeout se necessário
timeout(Duration::from_secs(3600), sr.read(&mut b)).await

// Adicionar heartbeat
if n == 0 {
    // Enviar chunk vazio para manter vivo
    let _ = stream.write_all(b"0\r\n\r\n").await;
}

// Não remover sessão se ainda há dados
if buffer.is_empty() {
    SESSIONS.lock().await.remove(&sid);
}
```

---

## Problema: Muita Memória Sendo Usada

### Sintomas
- Processo sdproxy-xhttp usa muita RAM
- OOM killer mata o processo
- Múltiplas conexões causam crash

### Causa
Buffer de reordenação crescendo indefinidamente ou vazamento de memória.

### Solução

```rust
// Limitar tamanho do buffer
const MAX_BUFFER_SIZE: usize = 100; // máximo de pacotes em buffer

if buffer.len() > MAX_BUFFER_SIZE {
    // Descartar pacotes muito antigos
    if let Some(min_key) = buffer.keys().next().copied() {
        if seq - min_key > MAX_BUFFER_SIZE as u64 {
            buffer.remove(&min_key);
        }
    }
}

// Limpar sessões inativas
tokio::spawn(async {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut sessions = SESSIONS.lock().await;
        sessions.retain(|_, s| *s.active.blocking_read());
    }
});
```

---

## Problema: Compilação Falha

### Sintomas
- Erro: `linker 'cc' not found`
- Erro: `could not compile 'openssl-sys'`
- Build script fails

### Causa
Dependências de sistema não instaladas.

### Solução

```bash
# Ubuntu/Debian
sudo apt install -y build-essential libssl-dev pkg-config

# RHEL/CentOS
sudo yum install -y gcc openssl-devel

# macOS
brew install openssl

# Depois recompilar
cargo build --bin sdproxy-xhttp --release
```

---

## Problema: Certificado TLS Auto-Assinado Rejeitado

### Sintomas
- Erro: "certificate verify failed"
- App não conecta com TLS
- "peer is incompatible"

### Causa
Cliente valida certificado SSL/TLS.

### Solução

```bash
# 1. Gerar novo certificado
openssl req -x509 -newkey rsa:2048 \
  -keyout /opt/sdproxy/key.pem \
  -out /opt/sdproxy/cert.pem \
  -days 365 -nodes \
  -subj "/CN=sdproxy/O=SDProxy/C=BR"

# 2. Verificar certificado
openssl x509 -in /opt/sdproxy/cert.pem -text -noout

# 3. Testar com curl
curl -k https://localhost:443/checkUser?user=test
```

---

## Checklist de Debug

- [ ] Servidor está rodando na porta 443?
- [ ] SSH backend está acessível em 127.0.0.1:22?
- [ ] Certificados TLS existem em `/opt/sdproxy/`?
- [ ] Firewall permite porta 443?
- [ ] Logs mostram "XHTTP download started"?
- [ ] BTreeMap está reordenando pacotes?
- [ ] CheckUser retorna JSON válido?
- [ ] Sessão XHTTP é criada com ID correto?
- [ ] GET recebe dados do SSH?
- [ ] POST envia dados sequenciados?

---

## Logs Úteis

```bash
# Ver logs em tempo real
journalctl -u proxy-443.service -f

# Ver últimas 100 linhas
journalctl -u proxy-443.service -n 100

# Ver com timestamp
journalctl -u proxy-443.service --no-pager

# Salvar em arquivo
journalctl -u proxy-443.service > /tmp/sdproxy.log

# Filtrar por palavra-chave
journalctl -u proxy-443.service | grep "XHTTP"
```

---

## Contato e Suporte

- **GitHub:** https://github.com/PedroJbk/SCrpro2
- **Issues:** Reportar problemas no GitHub
- **Documentação:** Ver README.md e IMPLEMENTATION_SUMMARY.md

---

**Versão:** 1.0  
**Data:** 29 de Julho de 2026  
**Status:** Guia de Troubleshooting
