A integração da API do DTunnel com o servidor funciona como uma ponte de comunicação entre o painel de gerenciamento (web) e o protocolo instalado no Linux (VPS). Com base no vídeo, aqui está o funcionamento detalhado:

### 1. Modos de Integração e Autenticação
O servidor DTunnel oferece quatro modos principais de autenticação, que definem como a "API" ou o sistema validará o usuário:
*   **Arquivo (File):** Utiliza um arquivo local (`/var/lib/proto-server/credentials.json`) para armazenar usuários e senhas.
*   **URL Personalizada (API Externa):** Este é o ponto real de integração de API. O servidor pode ser configurado para consultar uma URL externa (endpoint) para validar se um usuário e senha são válidos.
*   **SSH:** Integra-se diretamente com os usuários criados no sistema operacional Linux da VPS.
*   **Sem Autenticação:** Permite conexão livre.

### 2. Endpoints e Validação
Embora o vídeo não mostre o código-fonte da API, ele detalha como os dados são enviados e validados:

*   **Validação de Token:** Ao iniciar o script (`proto`), o primeiro passo de "integração" é a validação de um token de acesso gerado via bot. O servidor consulta um endpoint central para verificar se aquela instância está autorizada a rodar.
*   **Status do Servidor:** O menu "Status & Configuração" (Opção 4) exibe em tempo real se o protocolo está `ONLINE`, a porta em uso, a sub-rede virtual e quais protocolos (TCP, UDP, QUIC, XHTTP) estão ativos.
*   **Validação de Usuário (Modo URL):** Quando configurado no modo "URL personalizada", o servidor DTunnel envia as credenciais fornecidas pelo aplicativo cliente para o endpoint definido. Se a API retornar um status de sucesso (geralmente HTTP 200), a conexão é permitida.

### 3. Configuração no Painel (Frontend da API)
No painel de gerenciamento mostrado ao final do vídeo, a integração é feita preenchendo os seguintes campos que a API do protocolo interpreta:
*   **Modo de Conexão:** Selecionado como "DTunnel".
*   **Protocolo:** Define se a comunicação será via TCP (porta 80/8000), XHTTP (porta 443) ou outros.
*   **Payload e SNI:** Endpoints de rede usados para contornar restrições de operadoras.
*   **Porta do Proxy:** O painel comunica ao servidor em qual porta ele deve escutar (ex: porta 80 para conexões HTTP que fazem o túnel para a porta 8000 do serviço).

### Resumo do Fluxo:
1.  O **App Cliente** solicita conexão enviando usuário/senha.
2.  O **Servidor DTunnel** recebe e, dependendo do modo, consulta a **API (URL personalizada)** ou o **sistema (SSH)**.
3.  A **API** responde com a validação.
4.  O **Servidor** libera o tráfego através do túnel configurado.