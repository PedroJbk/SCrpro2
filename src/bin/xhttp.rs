use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{timeout, Duration};
use native_tls::Identity;
use tokio_native_tls::TlsAcceptor;
use serde::{Deserialize, Serialize};

/// Tipo de erro unificado para o projeto
type XhttpError = Box<dyn std::error::Error + Send + Sync>;

/// Limite de segurança para o corpo de um POST
#[allow(dead_code)]
const MAX_BODY_SIZE: usize = 32 * 1024 * 1024; // 32MB

/// Sessão xHTTP ativa com canais para comunicação GET<->POST<->SSH
struct XhttpSession {
    post_tx: mpsc::Sender<(u64, Vec<u8>)>,
    #[allow(dead_code)]
    get_tx: mpsc::Sender<Vec<u8>>,
    #[allow(dead_code)]
    active: Arc<RwLock<bool>>,
}

static SESSIONS: once_cell::sync::Lazy<Arc<Mutex<HashMap<String, XhttpSession>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

static ANON_SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn gen_anon_session_id() -> String {
    let n = ANON_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("anon-{}-{}", std::process::id(), n)
}

#[derive(Serialize, Deserialize)]
struct CheckUserResponse {
    username: String,
    count_connection: String,
    expiration_date: String,
    expiration_days: String,
    limit_connection: String,
}

#[tokio::main]
async fn main() -> Result<(), XhttpError> {
    let port = get_port();
    let status = get_status();
    let ssh_port = get_ssh_port();

    println!("[SDProxy] xHTTP v4.0.0 (DTunnel API + Sequence Fixed)");
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

const HEADER_READ_TIMEOUT_SECS: u64 = 15;
const MAX_HEADER_BYTES: usize = 64 * 1024;

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

    let acceptor = build_tls_acceptor(cert_path, key_path)?;
    let mut tls_stream = acceptor.accept(stream).await.map_err(|e| Box::new(e) as XhttpError)?;

    let data = match read_http_headers(&mut tls_stream).await {
        Some(d) => d,
        None => return handle_ssh_direct_tls(tls_stream, ssh_port, None).await,
    };

    let http_str = String::from_utf8_lossy(&data);

    if http_str.contains("GET ") {
        if let Some((_, path)) = parse_http_request(&http_str) {
            if path.contains("/checkUser") || path.contains("/user") {
                return handle_check_user(&mut tls_stream, &path).await;
            }
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
            if path.contains("/checkUser") || path.contains("/user") {
                return handle_check_user(&mut stream, &path).await;
            }
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

async fn handle_ssh_direct_tls(tls_stream: tokio_native_tls::TlsStream<TcpStream>, ssh_port: u16, initial_data: Option<Vec<u8>>) -> Result<(), XhttpError> {
    let mut ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    if let Some(data) = initial_data {
        ssh.write_all(&data).await.map_err(|e| Box::new(e) as XhttpError)?;
    }
    let (mut r, mut w) = tokio::io::split(tls_stream);
    let (mut sr, mut sw) = ssh.into_split();
    let _ = tokio::join!(tokio::io::copy(&mut r, &mut sw), tokio::io::copy(&mut sr, &mut w));
    Ok(())
}

async fn handle_check_user<S>(stream: &mut S, path: &str) -> Result<(), XhttpError> 
where S: AsyncWriteExt + Unpin 
{
    let user = path.split("user=").nth(1).unwrap_or("").split('&').next().unwrap_or("");
    
    // Tenta validar na API oficial do DTunnel se fornecida
    let client = reqwest::Client::new();
    let api_url = format!("https://api.dtunnel.com.br/api/checkUser?user={}", user);
    
    let resp_json = match timeout(Duration::from_secs(5), client.get(&api_url).send()).await {
        Ok(Ok(resp)) => {
            if resp.status().is_success() {
                resp.text().await.unwrap_or_default()
            } else {
                create_dummy_json(user)
            }
        },
        _ => create_dummy_json(user),
    };

    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{}",
        resp_json.len(),
        resp_json
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn create_dummy_json(user: &str) -> String {
    let data = CheckUserResponse {
        username: user.to_string(),
        count_connection: "1".to_string(),
        expiration_date: "31/12/2026".to_string(),
        expiration_days: "999".to_string(),
        limit_connection: "1".to_string(),
    };
    serde_json::to_string(&data).unwrap_or_default()
}

async fn write_final_chunk<W: AsyncWriteExt + Unpin>(w: &mut W) {
    let _ = w.write_all(b"0\r\n\r\n").await;
    let _ = w.flush().await;
}

fn resolve_session_id(path: &str) -> String {
    let (sid, _) = extract_path_info(path);
    if sid.is_empty() { gen_anon_session_id() } else { sid }
}

async fn handle_xhttp_get_tls(
    tls: &mut tokio_native_tls::TlsStream<TcpStream>,
    path: &str,
    status: &str,
    ssh_port: u16
) -> Result<(), XhttpError> {
    let sid = resolve_session_id(path);

    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let _ = ssh.set_nodelay(true);
    let (mut sr, mut sw) = ssh.into_split();

    let (ptx, mut prx) = mpsc::channel::<(u64, Vec<u8>)>(1024);
    let (gtx, mut grx) = mpsc::channel::<Vec<u8>>(1024);
    let act = Arc::new(RwLock::new(true));
    SESSIONS.lock().await.insert(sid.clone(), XhttpSession { post_tx: ptx, get_tx: gtx.clone(), active: act.clone() });

    let act_c = act.clone();
    tokio::spawn(async move {
        let mut next_seq = 0u64;
        let mut buffer = BTreeMap::new();
        
        while let Some((seq, data)) = prx.recv().await {
            if !*act_c.read().await { break; }
            buffer.insert(seq, data);
            
            while let Some(d) = buffer.remove(&next_seq) {
                if sw.write_all(&d).await.is_err() { break; }
                next_seq += 1;
            }
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
    lock.remove(&sid);
    Ok(())
}

async fn handle_xhttp_get_raw(stream: &mut TcpStream, path: &str, status: &str, ssh_port: u16) -> Result<(), XhttpError> {
    let sid = resolve_session_id(path);
    let ssh = TcpStream::connect(format!("127.0.0.1:{}", ssh_port)).await.map_err(|e| Box::new(e) as XhttpError)?;
    let (mut sr, mut sw) = ssh.into_split();

    let (ptx, mut prx) = mpsc::channel::<(u64, Vec<u8>)>(1024);
    let (gtx, mut grx) = mpsc::channel::<Vec<u8>>(1024);
    let act = Arc::new(RwLock::new(true));
    SESSIONS.lock().await.insert(sid.clone(), XhttpSession { post_tx: ptx, get_tx: gtx.clone(), active: act.clone() });

    tokio::spawn(async move {
        let mut next_seq = 0u64;
        let mut buffer = BTreeMap::new();
        while let Some((seq, data)) = prx.recv().await {
            buffer.insert(seq, data);
            while let Some(d) = buffer.remove(&next_seq) {
                if sw.write_all(&d).await.is_err() { break; }
                next_seq += 1;
            }
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
    stream.write_all(resp.as_bytes()).await?;

    let msg = "XHTTP download started\n";
    stream.write_all(format!("{:x}\r\n{}\r\n", msg.len(), msg).as_bytes()).await?;
    
    while let Some(d) = grx.recv().await {
        let _ = stream.write_all(format!("{:x}\r\n", d.len()).as_bytes()).await;
        let _ = stream.write_all(&d).await;
        let _ = stream.write_all(b"\r\n").await;
        let _ = stream.flush().await;
    }
    write_final_chunk(stream).await;
    SESSIONS.lock().await.remove(&sid);
    Ok(())
}

async fn handle_xhttp_post_tls(tls: &mut tokio_native_tls::TlsStream<TcpStream>, req: &[u8], path: &str, _: &str) -> Result<(), XhttpError> {
    let (sid, seq) = extract_path_info(path);
    let cl = extract_content_length_from_bytes(req).unwrap_or(0);
    let h_end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0) + 4;
    let mut body = req[h_end..].to_vec();

    let mut chunk = vec![0u8; 16384];
    while body.len() < cl {
        let want = std::cmp::min(chunk.len(), cl - body.len());
        let n = tls.read(&mut chunk[..want]).await?;
        if n == 0 { break; }
        body.extend_from_slice(&chunk[..n]);
    }

    send_to_session(&sid, seq.unwrap_or(0), body).await;
    tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").await?;
    Ok(())
}

async fn handle_xhttp_post_raw(stream: &mut TcpStream, req: &[u8], path: &str, _: &str) -> Result<(), XhttpError> {
    let (sid, seq) = extract_path_info(path);
    let cl = extract_content_length_from_bytes(req).unwrap_or(0);
    let h_end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0) + 4;
    let mut body = req[h_end..].to_vec();

    let mut chunk = vec![0u8; 16384];
    while body.len() < cl {
        let want = std::cmp::min(chunk.len(), cl - body.len());
        let n = stream.read(&mut chunk[..want]).await?;
        if n == 0 { break; }
        body.extend_from_slice(&chunk[..n]);
    }

    send_to_session(&sid, seq.unwrap_or(0), body).await;
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await?;
    Ok(())
}

async fn send_to_session(sid: &str, seq: u64, body: Vec<u8>) {
    let lock = SESSIONS.lock().await;
    if let Some(s) = lock.get(sid) {
        let _ = s.post_tx.send((seq, body)).await;
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

fn build_tls_acceptor(cp: &str, kp: &str) -> Result<TlsAcceptor, XhttpError> {
    let cert_pem = std::fs::read(cp).map_err(|e| Box::new(e) as XhttpError)?;
    let key_pem = std::fs::read(kp).map_err(|e| Box::new(e) as XhttpError)?;
    let identity = Identity::from_pkcs8(&cert_pem, &key_pem).map_err(|e| Box::new(e) as XhttpError)?;
    let native_acceptor = native_tls::TlsAcceptor::new(identity).map_err(|e| Box::new(e) as XhttpError)?;
    Ok(TlsAcceptor::from(native_acceptor))
}

fn get_port() -> u16 { std::env::args().enumerate().find(|(_, a)| a == "--port" || a == "-p").and_then(|(i, _)| std::env::args().nth(i+1)).and_then(|a| a.parse().ok()).unwrap_or(443) }
fn get_ssh_port() -> u16 { std::env::args().enumerate().find(|(_, a)| a == "--ssh-port").and_then(|(i, _)| std::env::args().nth(i+1)).and_then(|a| a.parse().ok()).unwrap_or(22) }
fn get_status() -> String { std::env::args().enumerate().find(|(_, a)| a == "--status" || a == "-s").and_then(|(i, _)| std::env::args().nth(i+1)).unwrap_or("@SDProxy".to_string()) }
