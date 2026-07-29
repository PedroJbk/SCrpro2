use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{timeout, Duration};

use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

/// Tipo de erro unificado para o projeto
type XhttpError = Box<dyn std::error::Error + Send + Sync>;

/// Limite de segurança para o corpo de um POST (evita alocação/OOM por Content-Length forjado)
const MAX_BODY_SIZE: usize = 32 * 1024 * 1024; // 32MB

/// Sessão xHTTP ativa com canais para comunicação GET<->POST<->SSH
#[allow(dead_code)]
struct XhttpSession {
    post_tx: mpsc::Sender<Vec<u8>>,
    get_tx: mpsc::Sender<Vec<u8>>,
    active: Arc<RwLock<bool>>,
}

static SESSIONS: once_cell::sync::Lazy<Arc<Mutex<HashMap<String, XhttpSession>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Contador global para gerar IDs de sessão únicos quando o cliente não informa um no path,
/// evitando que múltiplas conexões concorrentes colidam na chave "default".
static ANON_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn gen_anon_session_id() -> String {
    let n = ANON_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("anon-{}-{}", std::process::id(), n)
}

#[tokio::main]
async fn main() -> Result<(), XhttpError> {
    let port = get_port();
    let status = get_status();
    let ssh_port = get_ssh_port();

    println!("[BDRProxy] xHTTP v3.5.0 (DTunnel Final - fixed)");
    println!("[xHTTP] Porta: {} | SSH Backend: 127.0.0.1:{}", port, ssh_port);

    let listener = TcpListener::bind(format!("[::]:{}", port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let status_arc = Arc::new(status);

    loop {
        match listener.accept().await {
            Ok((client_stream, addr)) => {
                let _ = client_stream.set_nodelay(true);
                let status = status_arc.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_xhttp_client(client_stream, &status, ssh_port).await {
                        let err_str = e.to_string();
                        if !err_str.contains("Broken pipe") && !err_str.contains("Connection reset") {
                            println!("[xHTTP] Info {}: {}", addr, e);
                        }
                    }
                });
            }
            Err(e) => {
                println!("[xHTTP] Erro aceitar conexao: {}", e);
            }
        }
    }
}

async fn handle_xhttp_client(
    stream: TcpStream,
    status: &str,
    ssh_port: u16,
) -> Result<(), XhttpError> {
    let mut peek_buf = [0u8; 3];
    let peek_result = timeout(Duration::from_secs(10), stream.peek(&mut peek_buf)).await;
    let bytes_peeked = match peek_result {
        Ok(Ok(n)) => n,
        _ => return Ok(()),
    };

    if bytes_peeked == 0 { return Ok(()); }
    let first_byte = peek_buf[0];

    if first_byte == 0x16 {
        return handle_tls_dual(stream, status, ssh_port).await;
    }

    if first_byte >= 0x41 && first_byte <= 0x5A {
        return handle_http_dual_raw(stream, status, ssh_port).await;
    }

    handle_ssh_direct(stream, ssh_port).await
}

/// Tempo máximo total esperando os headers HTTP completos chegarem.
/// Em 3G/sinal fraco a requisição pode chegar fragmentada em vários pacotes
/// TCP; um único `read()` costuma pegar só um pedaço. 15s dá folga para
/// redes ruins sem deixar a conexão pendurada para sempre.
const HEADER_READ_TIMEOUT_SECS: u64 = 15;
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Lê do stream em loop, acumulando bytes até encontrar o fim dos headers
/// HTTP ("\r\n\r\n") ou até estourar o timeout/tamanho máximo. Substitui o
/// antigo "um read só" que quebrava sempre que a requisição chegava
/// fragmentada em mais de um segmento TCP.
async fn read_http_headers<S>(stream: &mut S) -> Option<Vec<u8>>
where
    S: AsyncReadExt + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let deadline = Duration::from_secs(HEADER_READ_TIMEOUT_SECS);

    loop {
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Some(buf);
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return None;
        }
        match timeout(deadline, stream.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => {
                // conexão fechada, erro de leitura, ou timeout total estourado
                return if buf.is_empty() { None } else { Some(buf) };
            }
        }
    }
}

async fn handle_tls_dual(
    stream: TcpStream,
    status: &str,
    ssh_port: u16,
) -> Result<(), XhttpError> {
    let cert_path = "/opt/sdproxy/cert.pem";
    let key_path = "/opt/sdproxy/key.pem";

    let config = build_tls_config(cert_path, key_path)?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let mut tls_stream = acceptor.accept(stream).await.map_err(|e| Box::new(e) as XhttpError)?;

    let data = match read_http_headers(&mut tls_stream).await {
        Some(d) => d,
        None => return handle_ssh_direct_tls(tls_stream, ssh_port, None).await,
    };

    let http_str = String::from_utf8_lossy(&data);

    if http_str.contains("GET ") {
        if let Some((_, path)) = parse_http_request(&http_str) {
            return handle_xhttp_get_tls(&mut tls_stream, &path, status, ssh_port).await;
        }
    } else if http_str.contains("POST ") {
        if let Some((_, path)) = parse_http_request(&http_str) {
            return handle_xhttp_post_tls(&mut tls_stream, &data, &path, status).await;
        }
    }

    if http_str.contains("HTTP/1.") {
        let resp = format!("HTTP/1.1 101 ({})\r\n\r\nHTTP/1.1 200 ({})\r\n\r\n", status, status);
        tls_stream.write_all(resp.as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;
        return handle_ssh_direct_tls(tls_stream, ssh_port, None).await;
    }

    handle_ssh_direct_tls(tls_stream, ssh_port, Some(data)).await
}

async fn handle_http_dual_raw(mut stream: TcpStream, status: &str, ssh_port: u16) -> Result<(), XhttpError> {
    let data = match read_http_headers(&mut stream).await {
        Some(d) => d,
        None => return handle_ssh_direct(stream, ssh_port).await,
    };
    let http_str = String::from_utf8_lossy(&data);

    if http_str.contains("GET ") {
        if let Some((_, path)) = parse_http_request(&http_str) {
            return handle_xhttp_get_raw(&mut stream, &path, status, ssh_port).await;
        }
    } else if http_str.contains("POST ") {
        if let Some((_, path)) = parse_http_request(&http_str) {
            return handle_xhttp_post_raw(&mut stream, &data, &path, status).await;
        }
    }

    if http_str.contains("HTTP/1.") {
        let resp = format!("HTTP/1.1 101 ({})\r\n\r\nHTTP/1.1 200 ({})\r\n\r\n", status, status);
        stream.write_all(resp.as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;
    }

    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let (mut r, mut w) = stream.into_split();
    let (mut sr, mut sw) = ssh.into_split();
    let _ = tokio::join!(tokio::io::copy(&mut r, &mut sw), tokio::io::copy(&mut sr, &mut w));
    Ok(())
}

async fn handle_ssh_direct(stream: TcpStream, ssh_port: u16) -> Result<(), XhttpError> {
    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let (mut r, mut w) = stream.into_split();
    let (mut sr, mut sw) = ssh.into_split();
    let _ = tokio::join!(tokio::io::copy(&mut r, &mut sw), tokio::io::copy(&mut sr, &mut w));
    Ok(())
}

async fn handle_ssh_direct_tls(tls_stream: tokio_rustls::server::TlsStream<TcpStream>, ssh_port: u16, initial_data: Option<Vec<u8>>) -> Result<(), XhttpError> {
    let mut ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    if let Some(data) = initial_data {
        ssh.write_all(&data).await.map_err(|e| Box::new(e) as XhttpError)?;
    }
    let (mut r, mut w) = tokio::io::split(tls_stream);
    let (mut sr, mut sw) = ssh.into_split();
    let _ = tokio::join!(tokio::io::copy(&mut r, &mut sw), tokio::io::copy(&mut sr, &mut w));
    Ok(())
}

// --- XHTTP Acceleration Logic ---

/// Escreve o chunk final "0\r\n\r\n" que finaliza corretamente uma resposta
/// HTTP com Transfer-Encoding: chunked. Sem isso o cliente fica esperando
/// o fim do stream indefinidamente quando a sessão SSH termina.
async fn write_final_chunk<W: AsyncWriteExt + Unpin>(w: &mut W) {
    let _ = w.write_all(b"0\r\n\r\n").await;
    let _ = w.flush().await;
}

/// Resolve o ID de sessão a partir do path. Se vazio, gera um ID único
/// por conexão em vez de usar uma chave fixa "default" que colidiria
/// entre clientes concorrentes.
fn resolve_session_id(path: &str) -> String {
    let (sid, _) = extract_path_info(path);
    if sid.is_empty() { gen_anon_session_id() } else { sid }
}

async fn handle_xhttp_get_tls(
    tls: &mut tokio_rustls::server::TlsStream<TcpStream>,
    path: &str,
    status: &str,
    ssh_port: u16
) -> Result<(), XhttpError> {
    let sid = resolve_session_id(path);

    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = ssh.set_nodelay(true);
    let (mut sr, mut sw) = ssh.into_split();

    let (ptx, mut prx) = mpsc::channel::<Vec<u8>>(1024);
    let (gtx, mut grx) = mpsc::channel::<Vec<u8>>(1024);
    let act = Arc::new(RwLock::new(true));
    SESSIONS.lock().await.insert(sid.clone(), XhttpSession { post_tx: ptx, get_tx: gtx.clone(), active: act.clone() });

    let act_c = act.clone();
    tokio::spawn(async move {
        while let Some(d) = prx.recv().await {
            if !*act_c.read().await { break; }
            if sw.write_all(&d).await.is_err() { break; }
        }
        // Canal do POST fechado (sessão encerrada ou peer sumiu): fecha o lado
        // de escrita da conexão SSH para não deixar o backend pendurado.
        let _ = sw.shutdown().await;
    });

    let gtx_c = gtx.clone();
    tokio::spawn(async move {
        let mut b = vec![0u8; 16384];
        while let Ok(Ok(n)) = timeout(Duration::from_secs(600), sr.read(&mut b)).await {
            if n == 0 || gtx_c.send(b[..n].to_vec()).await.is_err() { break; }
        }
    });

    // DTUNNEL Fix: Cabeçalhos estritos do LKProxy
    let resp = format!(
        "HTTP/1.1 200 OK\r\n\
         Connection: keep-alive\r\n\
         Content-Type: application/octet-stream\r\n\
         Transfer-Encoding: chunked\r\n\
         Cache-Control: no-store, no-cache, must-revalidate\r\n\
         Pragma: no-cache\r\n\
         Expires: 0\r\n\
         X-Content-Type-Options: nosniff\r\n\
         X-Session-ID: {}\r\n\
         X-Status: {}\r\n\r\n",
        sid, status
    );

    tls.write_all(resp.as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;
    tls.flush().await?;

    // Marcador DTunnel
    let msg = "XHTTP download started\n";
    tls.write_all(format!("{:x}\r\n{}\r\n", msg.len(), msg).as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = tls.flush().await;

    while let Some(d) = grx.recv().await {
        if tls.write_all(format!("{:x}\r\n", d.len()).as_bytes()).await.is_err() { break; }
        if tls.write_all(&d).await.is_err() { break; }
        if tls.write_all(b"\r\n").await.is_err() { break; }
        let _ = tls.flush().await;
    }

    write_final_chunk(tls).await;

    let mut lock = SESSIONS.lock().await;
    if let Some(s) = lock.get(&sid) { *s.active.write().await = false; }
    lock.remove(&sid);
    Ok(())
}

async fn handle_xhttp_get_raw(stream: &mut TcpStream, path: &str, status: &str, ssh_port: u16) -> Result<(), XhttpError> {
    let sid = resolve_session_id(path);

    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = ssh.set_nodelay(true);
    let (mut sr, mut sw) = ssh.into_split();

    let (ptx, mut prx) = mpsc::channel::<Vec<u8>>(1024);
    let (gtx, mut grx) = mpsc::channel::<Vec<u8>>(1024);
    let act = Arc::new(RwLock::new(true));
    SESSIONS.lock().await.insert(sid.clone(), XhttpSession { post_tx: ptx, get_tx: gtx.clone(), active: act.clone() });

    let act_c = act.clone();
    tokio::spawn(async move {
        while let Some(d) = prx.recv().await {
            if !*act_c.read().await { break; }
            if sw.write_all(&d).await.is_err() { break; }
        }
        let _ = sw.shutdown().await;
    });

    let gtx_c = gtx.clone();
    tokio::spawn(async move {
        let mut b = vec![0u8; 16384];
        while let Ok(Ok(n)) = timeout(Duration::from_secs(600), sr.read(&mut b)).await {
            if n == 0 || gtx_c.send(b[..n].to_vec()).await.is_err() { break; }
        }
    });

    let resp = format!(
        "HTTP/1.1 200 OK\r\n\
         Connection: keep-alive\r\n\
         Content-Type: application/octet-stream\r\n\
         Transfer-Encoding: chunked\r\n\
         X-Session-ID: {}\r\n\
         X-Status: {}\r\n\r\n",
        sid, status
    );
    stream.write_all(resp.as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;

    let msg = "XHTTP download started\n";
    stream.write_all(format!("{:x}\r\n{}\r\n", msg.len(), msg).as_bytes()).await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = stream.flush().await;

    while let Some(d) = grx.recv().await {
        if stream.write_all(format!("{:x}\r\n", d.len()).as_bytes()).await.is_err() { break; }
        if stream.write_all(&d).await.is_err() { break; }
        if stream.write_all(b"\r\n").await.is_err() { break; }
        let _ = stream.flush().await;
    }

    write_final_chunk(stream).await;

    // Antes faltava esta limpeza no caminho "raw": sessões ficavam presas
    // no HashMap para sempre depois que o GET terminava.
    let mut lock = SESSIONS.lock().await;
    if let Some(s) = lock.get(&sid) { *s.active.write().await = false; }
    lock.remove(&sid);
    Ok(())
}

async fn handle_xhttp_post_tls(tls: &mut tokio_rustls::server::TlsStream<TcpStream>, req: &[u8], path: &str, _: &str) -> Result<(), XhttpError> {
    let sid = extract_path_info(path).0;

    let cl = extract_content_length_from_bytes(req).unwrap_or(0);
    if cl > MAX_BODY_SIZE {
        tls.write_all(b"HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\n\r\n").await.map_err(|e| Box::new(e) as XhttpError)?;
        return Ok(());
    }
    let h_end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0) + 4;
    let mut body = req[h_end..].to_vec();

    let mut chunk = vec![0u8; 16384];
    while body.len() < cl {
        let want = std::cmp::min(chunk.len(), cl - body.len());
        let n = tls.read(&mut chunk[..want]).await.map_err(|e| Box::new(e) as XhttpError)?;
        if n == 0 { break; }
        body.extend_from_slice(&chunk[..n]);
    }

    send_to_session(&sid, body).await;

    tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = tls.flush().await;
    Ok(())
}

async fn handle_xhttp_post_raw(stream: &mut TcpStream, req: &[u8], path: &str, _: &str) -> Result<(), XhttpError> {
    let sid = extract_path_info(path).0;

    let cl = extract_content_length_from_bytes(req).unwrap_or(0);
    if cl > MAX_BODY_SIZE {
        stream.write_all(b"HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\n\r\n").await.map_err(|e| Box::new(e) as XhttpError)?;
        return Ok(());
    }
    let h_end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0) + 4;
    let mut body = req[h_end..].to_vec();

    let mut chunk = vec![0u8; 16384];
    while body.len() < cl {
        let want = std::cmp::min(chunk.len(), cl - body.len());
        let n = stream.read(&mut chunk[..want]).await.map_err(|e| Box::new(e) as XhttpError)?;
        if n == 0 { break; }
        body.extend_from_slice(&chunk[..n]);
    }

    send_to_session(&sid, body).await;

    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = stream.flush().await;
    Ok(())
}

/// Envia o corpo do POST para a sessão correspondente (uma única aquisição de lock,
/// em vez das duas sequenciais do código original). Cai para "default" apenas
/// como último recurso de compatibilidade com clientes antigos.
async fn send_to_session(sid: &str, body: Vec<u8>) {
    let lock = SESSIONS.lock().await;
    if let Some(s) = lock.get(sid) {
        let _ = s.post_tx.send(body).await;
    } else if !sid.is_empty() {
        if let Some(s) = lock.get("default") {
            let _ = s.post_tx.send(body).await;
        }
    }
}

fn parse_http_request(data: &str) -> Option<(String, String)> {
    let line = data.lines().next()?;
    let p: Vec<&str> = line.split_whitespace().collect();
    if p.len() >= 2 { Some((p[0].to_string(), p[1].to_string())) } else { None }
}

fn extract_path_info(path: &str) -> (String, Option<u64>) {
    let p = path.split('?').next().unwrap_or(path).trim_start_matches('/').split('/').collect::<Vec<&str>>();
    if p.is_empty() || p[0].is_empty() { return (String::new(), None); }
    if p.len() >= 2 {
        if ["ssh", "xhttp", "split"].contains(&p[0]) {
            return (p[1].to_string(), if p.len() >= 3 { p[2].parse().ok() } else { None });
        }
        return (p[0].to_string(), p[1].parse().ok());
    }
    (p[0].to_string(), None)
}

fn extract_content_length_from_bytes(data: &[u8]) -> Option<usize> {
    let s = String::from_utf8_lossy(data);
    for l in s.lines() { if l.to_lowercase().starts_with("content-length:") { return l.split(':').nth(1)?.trim().parse().ok(); } }
    None
}

fn build_tls_config(cp: &str, kp: &str) -> Result<rustls::ServerConfig, XhttpError> {
    let certs: Vec<Certificate> = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(cp).map_err(|e| Box::new(e) as XhttpError)?)).map_err(|e| Box::new(e) as XhttpError)?.into_iter().map(Certificate).collect();
    let keys: Vec<PrivateKey> = rustls_pemfile::pkcs8_private_keys(&mut std::io::BufReader::new(std::fs::File::open(kp).map_err(|e| Box::new(e) as XhttpError)?)).map_err(|e| Box::new(e) as XhttpError)?.into_iter().map(PrivateKey).collect();
    if certs.is_empty() || keys.is_empty() { return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Certs empty")) as XhttpError); }
    let mut c = rustls::ServerConfig::builder().with_safe_defaults().with_no_client_auth().with_single_cert(certs, keys.into_iter().next().unwrap()).map_err(|e| Box::new(e) as XhttpError)?;

    c.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(c)
}

fn get_port() -> u16 { std::env::args().enumerate().find(|(_, a)| a == "--port" || a == "-p").and_then(|(i, _)| std::env::args().nth(i+1)).and_then(|a| a.parse().ok()).unwrap_or(443) }
fn get_ssh_port() -> u16 { std::env::args().enumerate().find(|(_, a)| a == "--ssh-port").and_then(|(i, _)| std::env::args().nth(i+1)).and_then(|a| a.parse().ok()).unwrap_or(22) }
fn get_status() -> String { std::env::args().enumerate().find(|(_, a)| a == "--status" || a == "-s").and_then(|(i, _)| std::env::args().nth(i+1)).unwrap_or("@SDProxy".to_string()) }
