# Saga Pattern — Cadastro de Usuários com Assinatura

> Planejamento arquitetural completo aplicando o padrão **Saga Orquestrada** para garantir consistência distribuída entre o cadastro de usuário e a criação de assinatura, sem depender de transação distribuída (2PC) e sem deixar estados inconsistentes visíveis ao usuário.

---

## Sumário

1. [Por que Saga?](#1-por-que-saga)
2. [Escolha: Saga Orquestrada](#2-escolha-saga-orquestrada)
3. [Definição da Saga](#3-definição-da-saga)
4. [Estados e Transições do Usuário](#4-estados-e-transições-do-usuário)
5. [Fluxo Completo — Caminho Feliz](#5-fluxo-completo--caminho-feliz)
6. [Fluxos de Compensação](#6-fluxos-de-compensação)
7. [O Problema do Retry — Idempotência](#7-o-problema-do-retry--idempotência)
8. [Arquitetura dos Serviços](#8-arquitetura-dos-serviços)
9. [Modelo de Dados](#9-modelo-de-dados)
10. [Migração do Monólito](#10-migração-do-monólito)
11. [Rollout Incremental](#11-rollout-incremental)
12. [Trade-offs](#12-trade-offs)

---

## 1. Por que Saga?

O problema exige coordenar **duas operações em serviços distintos** — criar usuário e criar assinatura — como se fossem uma única transação lógica:

- Se o usuário for criado mas a assinatura falhar → o usuário não pode ficar "preso" em estado inconsistente
- Se o usuário tentar novamente com o mesmo email → não pode receber erro de "E-mail já cadastrado"
- A confirmação deve ser **síncrona** (requisito do PO) — não é possível usar filas assíncronas

Uma transação distribuída clássica (2PC — Two-Phase Commit) resolveria a atomicidade, mas introduz acoplamento forte entre serviços, bloqueios de longa duração e é inviável quando os serviços têm bancos de dados diferentes.

O padrão **Saga** resolve isso decompondo a operação em etapas locais, cada uma com uma **transação de compensação** correspondente que desfaz o efeito caso algo falhe.

```mermaid
graph LR
    subgraph "Problema sem Saga"
        P1["Criar usuário\n✅ OK"] --> P2["Criar assinatura\n❌ FALHA"]
        P2 --> P3["Usuário existe\nAssinatura não existe\n⚠️ INCONSISTENTE"]
    end

    subgraph "Solução com Saga"
        S1["Criar usuário\nstatus: PENDING"] --> S2["Criar assinatura\n❌ FALHA"]
        S2 --> S3["Compensação:\nRemover usuário PENDING"]
        S3 --> S4["Estado limpo\nNada foi realizado\n✅ CONSISTENTE"]
    end
```

---

## 2. Escolha: Saga Orquestrada

Existem duas variações do padrão:

| | **Orquestrada** ✅ (escolha) | **Coreografada** |
|---|---|---|
| **Coordenação** | Um orquestrador central comanda cada etapa | Serviços emitem e reagem a eventos |
| **Estado da Saga** | Centralizado no orquestrador | Distribuído entre os serviços |
| **Compensação** | Orquestrador chama as compensações explicitamente | Cada serviço reage a eventos de falha |
| **Rastreabilidade** | ✅ Fácil — estado em um único lugar | Difícil — requer correlação de eventos |
| **Número de serviços** | Ideal para poucos serviços (2–4) | Ideal para muitos serviços |
| **Adequação ao problema** | ✅ Excelente — 2 serviços, fluxo síncrono | Introduz complexidade desnecessária |

**Motivo da escolha:** o fluxo envolve apenas dois serviços (User Service + Subscription Service), a confirmação precisa ser síncrona, e a rastreabilidade do estado é crítica para implementar a idempotência no retry.

---

## 3. Definição da Saga

A Saga é composta por **duas transações locais**, cada uma com sua compensação:

```mermaid
flowchart LR
    subgraph "Saga: Cadastro com Assinatura"
        direction LR
        T1["**T1** — Criar Usuário\nUser Service\nstatus: PENDING_SUBSCRIPTION"]
        T2["**T2** — Criar Assinatura\nSubscription Service\n{user_id, produto}"]
        T1 --> T2
    end

    subgraph "Compensações"
        direction LR
        C1["**C1** — Cancelar Usuário\nUser Service\nstatus: CANCELLED"]
        C2["**C2** — Cancelar Assinatura\nSubscription Service\n(se criada parcialmente)"]
    end

    T2 -- "falha em T2" --> C2
    C2 --> C1
    T1 -- "falha em T1" --> C1

    style T1 fill:#052e16,stroke:#059669,color:#6ee7b7
    style T2 fill:#052e16,stroke:#059669,color:#6ee7b7
    style C1 fill:#450a0a,stroke:#dc2626,color:#fca5a5
    style C2 fill:#450a0a,stroke:#dc2626,color:#fca5a5
```

### Tabela de transações e compensações

| Etapa | Serviço | Transação | Compensação |
|---|---|---|---|
| **T1** | User Service | `POST /users` → cria usuário com `status: PENDING_SUBSCRIPTION` | `PATCH /users/{id}` → `status: CANCELLED` |
| **T2** | Subscription Service | `POST /subscriptions` → cria assinatura vinculada ao `user_id` | `DELETE /subscriptions/{id}` → cancela assinatura |
| **Finalização** | User Service | `PATCH /users/{id}` → `status: ACTIVE` | — (ponto de não retorno) |

> **Ponto de não retorno:** após o `PATCH status: ACTIVE`, a operação é considerada commitada. Se algo falhar depois desse ponto, o usuário já está ativo e com assinatura — não há inconsistência.

---

## 4. Estados e Transições do Usuário

O status do usuário no User Service é o coração da solução. Ele reflete o estado da Saga em andamento.

```mermaid
stateDiagram-v2
    [*] --> PENDING_SUBSCRIPTION : T1 — POST /users<br/>(cadastro com assinatura)
    [*] --> ACTIVE : POST /users<br/>(cadastro simples, sem assinatura)

    PENDING_SUBSCRIPTION --> ACTIVE : T2 OK + PATCH status<br/>(Saga completada)
    PENDING_SUBSCRIPTION --> CANCELLED : Compensação C1<br/>(T2 falhou)

    CANCELLED --> [*] : Registro pode ser<br/>reutilizado no retry

    ACTIVE --> [*] : Usuário ativo<br/>ponto de não retorno

    note right of PENDING_SUBSCRIPTION
        Janela de inconsistência visível
        apenas internamente.
        O usuário nunca vê esse estado.
    end note

    note right of CANCELLED
        Email liberado para retry.
        signup_attempt_id preservado
        para identificar a tentativa.
    end note
```

---

## 5. Fluxo Completo — Caminho Feliz

### Cadastro simples (sem assinatura)

```mermaid
sequenceDiagram
    actor U as Usuário
    participant FE as Signup Frontend
    participant OR as Signup Orchestrator
    participant US as User Service
    participant PG as PostgreSQL (User)

    U->>FE: preenche email, nome, senha
    note over FE: gera signup_attempt_id (UUID)
    FE->>OR: POST /signup<br/>{ email, nome, senha, attempt_id }

    OR->>OR: verifica tabela signup_attempts<br/>(attempt_id já existe?)

    OR->>US: POST /users<br/>{ email, nome, senha_hash, status: ACTIVE }
    US->>PG: INSERT users
    PG-->>US: 201 Created
    US-->>OR: 201 { user_id }

    OR->>OR: salva attempt_id → COMPLETED
    OR-->>FE: 200 OK { user_id }
    FE-->>U: ✅ "Cadastro realizado!"
```

### Cadastro com assinatura — Saga completa

```mermaid
sequenceDiagram
    actor U as Usuário
    participant FE as Signup Frontend
    participant OR as Signup Orchestrator
    participant US as User Service
    participant SS as Subscription Service

    U->>FE: preenche dados + escolhe produto
    note over FE: gera signup_attempt_id (UUID)
    FE->>OR: POST /signup<br/>{ email, nome, senha, produto, attempt_id }

    OR->>OR: 1. verifica idempotência
    note over OR: attempt_id não existe → prosseguir

    rect rgb(5, 46, 22)
        note over OR,US: T1 — Transação Local: Criar Usuário
        OR->>US: POST /users<br/>{ email, nome, senha_hash,<br/>status: PENDING_SUBSCRIPTION }
        US-->>OR: 201 { user_id }
        OR->>OR: salva attempt_id → PROCESSING<br/>salva user_id para compensação
    end

    rect rgb(5, 46, 22)
        note over OR,SS: T2 — Transação Local: Criar Assinatura
        OR->>SS: POST /subscriptions<br/>{ user_id, produto }
        SS-->>OR: 201 { subscription_id }
    end

    rect rgb(5, 46, 22)
        note over OR,US: Finalização — Ativar Usuário
        OR->>US: PATCH /users/{user_id}<br/>{ status: ACTIVE }
        US-->>OR: 200 OK
    end

    OR->>OR: salva attempt_id → COMPLETED
    OR-->>FE: 200 OK { user_id, subscription_id }
    FE-->>U: ✅ "Cadastro + assinatura realizados!"
```

---

## 6. Fluxos de Compensação

### Cenário A — Falha em T1 (User Service indisponível)

```mermaid
sequenceDiagram
    participant OR as Signup Orchestrator
    participant US as User Service
    participant SS as Subscription Service

    OR->>US: POST /users { status: PENDING_SUBSCRIPTION }
    US-->>OR: ❌ 5xx / timeout

    note over OR: T1 falhou antes de criar qualquer coisa
    note over OR: Não há nada a compensar

    OR->>OR: salva attempt_id → FAILED
    OR-->>OR: retorna erro ao frontend<br/>"Tente novamente"
```

> Nenhuma compensação necessária. O usuário não foi criado. O retry com o mesmo `attempt_id` tentará novamente do zero.

---

### Cenário B — Falha em T2 (Subscription Service indisponível)

```mermaid
sequenceDiagram
    participant OR as Signup Orchestrator
    participant US as User Service
    participant SS as Subscription Service

    OR->>US: POST /users { status: PENDING_SUBSCRIPTION }
    US-->>OR: 201 { user_id }
    OR->>OR: salva user_id para rollback

    OR->>SS: POST /subscriptions { user_id, produto }
    SS-->>OR: ❌ 5xx / timeout

    note over OR: T2 falhou — executar compensação C1

    rect rgb(69, 10, 10)
        note over OR,US: C1 — Compensação: Cancelar Usuário
        OR->>US: PATCH /users/{user_id}<br/>{ status: CANCELLED }
        US-->>OR: 200 OK
    end

    OR->>OR: salva attempt_id → FAILED
    OR-->>OR: retorna erro ao frontend<br/>"Tente novamente"
```

---

### Cenário C — Falha no PATCH de ativação (após assinatura criada)

```mermaid
sequenceDiagram
    participant OR as Signup Orchestrator
    participant US as User Service
    participant SS as Subscription Service

    OR->>US: POST /users { status: PENDING_SUBSCRIPTION }
    US-->>OR: 201 { user_id }

    OR->>SS: POST /subscriptions { user_id, produto }
    SS-->>OR: 201 { subscription_id }
    OR->>OR: salva subscription_id para rollback

    OR->>US: PATCH /users/{user_id} { status: ACTIVE }
    US-->>OR: ❌ 5xx / timeout

    note over OR: Assinatura criada mas usuário não ativado
    note over OR: Executar compensação C2 + C1

    rect rgb(69, 10, 10)
        note over OR,SS: C2 — Compensação: Cancelar Assinatura
        OR->>SS: DELETE /subscriptions/{subscription_id}
        SS-->>OR: 204 OK
    end

    rect rgb(69, 10, 10)
        note over OR,US: C1 — Compensação: Cancelar Usuário
        OR->>US: PATCH /users/{user_id} { status: CANCELLED }
        US-->>OR: 200 OK
    end

    OR->>OR: salva attempt_id → FAILED
    OR-->>OR: retorna erro ao frontend
```

---

### Visão geral dos cenários de falha

```mermaid
flowchart TD
    START([POST /signup recebido]) --> T1

    T1["T1: POST /users\nstatus: PENDING"] --> T1_OK{T1 OK?}

    T1_OK -- "❌ Falha" --> ERR_T1["Nenhuma compensação\nnecessária\nattempt_id → FAILED"]
    T1_OK -- "✅ OK\nsalva user_id" --> T2

    T2["T2: POST /subscriptions\n{user_id, produto}"] --> T2_OK{T2 OK?}

    T2_OK -- "❌ Falha" --> C1_A["C1: PATCH users\nstatus: CANCELLED\nattempt_id → FAILED"]
    T2_OK -- "✅ OK\nsalva subscription_id" --> FIN

    FIN["PATCH /users\nstatus: ACTIVE"] --> FIN_OK{Patch OK?}

    FIN_OK -- "❌ Falha" --> C2["C2: DELETE subscriptions\n+ C1: PATCH users CANCELLED\nattempt_id → FAILED"]
    FIN_OK -- "✅ OK" --> SUCCESS["attempt_id → COMPLETED\n200 OK ao frontend\n✅ Saga concluída"]

    ERR_T1 --> FRONTEND_ERR(["❌ Erro ao frontend\n'Tente novamente'"])
    C1_A --> FRONTEND_ERR
    C2 --> FRONTEND_ERR

    style SUCCESS fill:#052e16,stroke:#059669,color:#6ee7b7
    style FRONTEND_ERR fill:#450a0a,stroke:#dc2626,color:#fca5a5
    style C1_A fill:#450a0a,stroke:#dc2626,color:#fca5a5
    style C2 fill:#450a0a,stroke:#dc2626,color:#fca5a5
    style ERR_T1 fill:#1c1500,stroke:#d97706,color:#fcd34d
```

---

## 7. O Problema do Retry — Idempotência

Este é o maior desafio: após uma falha, o usuário tenta novamente com o **mesmo email**. A compensação pode ter deixado um registro `CANCELLED` no banco. A solução usa **dois mecanismos combinados**.

### Mecanismo 1 — `signup_attempt_id`

O frontend gera um UUID ao **carregar o formulário** (não ao submeter). Esse UUID identifica a tentativa de cadastro e é enviado em todo request, inclusive nos retries.

### Mecanismo 2 — Tabela `signup_attempts` no Orchestrator

```sql
CREATE TYPE signup_attempt_status AS ENUM (
    'PROCESSING',
    'COMPLETED',
    'FAILED'
);
```

```sql
CREATE TABLE signup_attempts (
    attempt_id       UUID PRIMARY KEY,
    email            VARCHAR      NOT NULL,
    status           signup_attempt_status NOT NULL,
    user_id          UUID,                   -- preenchido após T1
    subscription_id  UUID,                   -- preenchido após T2
    created_at       TIMESTAMP    DEFAULT NOW(),
    updated_at       TIMESTAMP    DEFAULT NOW()
);
```

### Árvore de decisão para retries

```mermaid
flowchart TD
    A([POST /signup\ncom attempt_id]) --> B{attempt_id existe\nem signup_attempts?}

    B -- "Não" --> NOVO[Fluxo normal:\nexecuta a Saga do zero]

    B -- "Sim · COMPLETED" --> CACHE["Retorna 200 OK\ncom resultado cacheado\n(idempotente)"]

    B -- "Sim · PROCESSING" --> PROC["Saga ainda em andamento\n(caso raro: concorrência)\nRetorna 409 'Em processamento'"]

    B -- "Sim · FAILED" --> EMAIL{Email existe\nno User Service?}

    EMAIL -- "Não existe" --> NOVO

    EMAIL -- "Sim · ACTIVE\noutro attempt_id" --> CONFLICT["Erro 409\nEmail já cadastrado\n(outro usuário real)"]

    EMAIL -- "Sim · CANCELLED\nmesmo attempt_id" --> REUSE["Deleta registro CANCELLED\nRecria usuário do zero\n(email foi liberado pela compensação)"]

    EMAIL -- "Sim · PENDING\nmesmo attempt_id" --> RESUME["Retenta T2 apenas:\nPOST /subscriptions\n(T1 já foi executado)"]

    REUSE --> NOVO
    RESUME --> T2_RETRY["T2: POST /subscriptions\n{user_id, produto}"]

    style CACHE fill:#052e16,stroke:#059669,color:#6ee7b7
    style CONFLICT fill:#450a0a,stroke:#dc2626,color:#fca5a5
    style REUSE fill:#1c1500,stroke:#d97706,color:#fcd34d
    style RESUME fill:#0a1628,stroke:#3b82f6,color:#93c5fd
```

### Tabela de todos os cenários de retry

| Estado no DB | `attempt_id` | Email | Comportamento do Orchestrator |
|---|---|---|---|
| Não existe | Novo | Livre | Executa Saga do zero |
| `COMPLETED` | Mesmo | — | Retorna 200 OK cacheado |
| `FAILED` + usuário não existe | Mesmo | Livre | Executa Saga do zero |
| `FAILED` + usuário `CANCELLED` | Mesmo | Existente | Deleta `CANCELLED`, executa Saga do zero |
| `FAILED` + usuário `PENDING` | Mesmo | Existente | Retenta apenas T2 (assinatura) |
| `FAILED` + usuário `ACTIVE` | Diferente | Existente | Erro 409 — E-mail já cadastrado |
| `PROCESSING` | Mesmo | — | 409 "Em processamento" (evita dupla execução) |

---

## 8. Arquitetura dos Serviços

```mermaid
graph TD
    subgraph CLIENT["Cliente"]
        FE["Signup Frontend\n• Gera signup_attempt_id ao carregar form\n• Nunca reusa o mesmo attempt_id entre sessões\n• Exibe erro claro em caso de falha"]
    end

    subgraph ORCH_SVC["Signup Orchestrator"]
        OR_API["API REST\nPOST /signup"]
        OR_SAGA["Saga Engine\n• Executa T1 → T2 → Finalização\n• Detecta falhas e aciona compensações\n• Gerencia attempt_id"]
        OR_DB[("PostgreSQL\nsignup_attempts\n• attempt_id PK\n• email\n• status\n• user_id\n• subscription_id")]
        OR_API --> OR_SAGA
        OR_SAGA --- OR_DB
    end

    subgraph USER_SVC["User Service"]
        US_API["API REST\nPOST /users\nPATCH /users/{id}\nDELETE /users/{id}"]
        US_DB[("PostgreSQL\nusers\n• id UUID PK\n• email UNIQUE\n• nome\n• senha_hash\n• status\n• created_at")]
        US_API --- US_DB
    end

    subgraph SUB_SVC["Subscription Service\n(já existe)"]
        SS_API["API REST\nPOST /subscriptions\nDELETE /subscriptions/{id}"]
        SS_DB[("Oracle\nassinatura\n• id_assinatura PK\n• id_usuario\n• produto\n• status")]
        SS_API --- SS_DB
    end

    FE -- "POST /signup\n+ attempt_id" --> OR_API
    OR_SAGA -- "T1: POST /users\nC1: PATCH status CANCELLED" --> US_API
    OR_SAGA -- "T2: POST /subscriptions\nC2: DELETE /subscriptions/{id}" --> SS_API
```

### Contrato de API do Signup Orchestrator

```
POST /signup

Request:
{
  "email":           "usuario@email.com",
  "nome":            "João Silva",
  "senha":           "senha123",
  "attempt_id":      "550e8400-e29b-41d4-a716-446655440000",  // UUID gerado pelo frontend
  "produto":         "PLANO_BASICO"  // opcional — se presente, cria assinatura
}

Response 200 — sucesso:
{
  "user_id":         "7f3b2e1a-...",
  "subscription_id": "9c4d5f2b-..."  // presente apenas se produto foi selecionado
}

Response 409 — E-mail já cadastrado:
{
  "error": "EMAIL_ALREADY_EXISTS",
  "message": "Este e-mail já está em uso."
}

Response 4xx — falha na Saga (retry possível):
{
  "error": "SAGA_FAILED",
  "message": "Ocorreu um erro. Por favor, tente novamente.",
  "retryable": true
}
```

---

## 9. Modelo de Dados

```mermaid
erDiagram
    SIGNUP_ATTEMPTS {
        uuid attempt_id PK
        varchar email
        varchar status
        uuid user_id FK
        uuid subscription_id
        timestamp created_at
        timestamp updated_at
    }

    USERS {
        uuid id PK
        varchar email
        varchar nome
        varchar senha_hash
        varchar status
        timestamp created_at
    }

    ASSINATURA {
        uuid id_assinatura PK
        uuid id_usuario FK
        varchar produto
        varchar status
        timestamp created_at
    }

    SIGNUP_ATTEMPTS ||--o| USERS : "referencia"
    USERS ||--o| ASSINATURA : "possui"
```

**Status do usuário:**
- `PENDING_SUBSCRIPTION` — T1 executado, aguardando T2
- `ACTIVE` — Saga concluída (ou cadastro simples)
- `CANCELLED` — Compensação executada após falha em T2

**Status da tentativa (signup_attempts):**
- `PROCESSING` — Saga em andamento
- `COMPLETED` — Saga concluída com sucesso
- `FAILED` — Saga falhou, compensações executadas

---

## 10. Migração do Monólito

A estratégia usa o padrão **Strangler Fig**: o novo sistema cresce ao redor do monólito até substituí-lo completamente, sem downtime.

### Problema com Foreign Keys e JOINs

```mermaid
graph TD
    subgraph MONO["Monólito Oracle"]
        USR["Tabela: usuarios\nid_usuario PK"]
        EMP["Tabela: emprestimo\nid_usuario FK"]
        COB["Tabela: cobranca\nid_usuario FK"]
    end

    subgraph NEW["Novo Ecossistema"]
        PG[("PostgreSQL\nusers · UUID")]
        PROJ["Projeção de Usuários\nno Oracle\n(mantida por CDC)"]
    end

    SYNC["User Sync Worker\nCDC via Debezium"]

    USR -- "eventos de change" --> SYNC
    SYNC -- "replica" --> PG
    SYNC -- "atualiza" --> PROJ
    EMP -. "JOIN local\nsem HTTP" .-> PROJ
    COB -. "JOIN local\nsem HTTP" .-> PROJ
```

**Estratégia para JOINs:** o monólito continua fazendo JOINs diretamente no Oracle, usando uma **projeção local** (view materializada ou tabela espelho) mantida em sincronia pelo User Sync Worker via CDC. Zero mudança no código do monólito, zero latência extra.

### Fases da migração

```mermaid
flowchart LR
    subgraph F1["Fase 1\n2–3 semanas"]
        A["Criar User Service\n+ PostgreSQL\n+ Signup Orchestrator\n+ User Sync Worker CDC"]
    end

    subgraph F2["Fase 2\n1–2 semanas"]
        B["User Service com\nfallback ao Oracle\nNovos → PostgreSQL\nAntigos → fallback"]
    end

    subgraph F3["Fase 3\n1–2 semanas"]
        C["Nova SPA paralela\nA/B: 5%→20%→50%→100%\nMonitor SLOs\nRollback automático"]
    end

    subgraph F4["Fase 4\nmeses depois"]
        D["Remove fallback Oracle\nDesliga Sync Worker\nRemove código do monólito"]
    end

    F1 --> F2 --> F3 --> F4
```

---

## 11. Rollout Incremental

```mermaid
gantt
    title Rollout Incremental — Extração do Cadastro
    dateFormat YYYY-MM-DD
    axisFormat S%W

    section Infraestrutura
    User Service + PostgreSQL         :s1, 2025-01-01, 7d
    Signup Orchestrator + Saga        :s2, after s1, 7d
    User Sync Worker + CDC            :s3, after s1, 14d
    Validação paridade de dados       :s4, after s3, 7d

    section Frontend
    Nova SPA em /novo-cadastro        :s5, after s4, 7d
    Testes internos e QA              :s6, after s5, 7d

    section Rollout
    A/B 5% do tráfego                 :s7, after s6, 5d
    A/B 20% do tráfego                :s8, after s7, 5d
    A/B 50% do tráfego                :s9, after s8, 4d
    100% novo fluxo                   :s10, after s9, 7d

    section Limpeza
    Remover fallback Oracle           :s11, after s10, 14d
    Remover código do monólito        :s12, after s11, 14d
```

**Critérios de avanço:**
- Latência p99 do cadastro < 800ms
- Taxa de erro < 0.1%
- Paridade de dados Oracle ↔ PostgreSQL validada por amostragem diária
- Feature flag de rollback disponível a qualquer momento

---

## 12. Trade-offs

### Saga Orquestrada

| Aspecto | Decisão | Justificativa |
|---|---|---|
| **Consistência** | Eventual, com compensação | 2PC é inviável entre serviços com bancos distintos |
| **Visibilidade do estado** | `PENDING` visível internamente | Nunca exposto ao usuário final |
| **Janela de inconsistência** | Milissegundos a segundos | Aceitável — o usuário aguarda a resposta síncrona |
| **Acoplamento** | Orchestrator conhece os dois serviços | Trade-off intencional pela rastreabilidade |
| **Falha na compensação** | Job de retry para compensações pendentes | Compensação C1/C2 também pode falhar e precisa de retry |

### Status `PENDING_SUBSCRIPTION`

- ✅ Resolve o retry sem 2PC
- ✅ Permite distinguir retentativas legítimas de conflitos reais
- ⚠️ Requer **job de limpeza** de registros `PENDING` com mais de X horas (usuários "zumbis" de Sagas que nunca concluíram)

### Confirmação síncrona (requisito do PO)

- ✅ UX imediata — o usuário sabe na hora o resultado
- ⚠️ Latência do cadastro com assinatura depende do Subscription Service
- 🛡️ Mitigação: timeout configurado (ex: 5 segundos) com mensagem clara pedindo nova tentativa

### Projeção de usuários no Oracle

- ✅ Zero mudança no monólito — JOINs continuam funcionando localmente
- ✅ Zero aumento de latência nas telas existentes
- ⚠️ Lag de CDC: dados podem estar levemente desatualizados (segundos)
- ✅ Aceitável para telas não-críticas como listagem de empréstimos

---

## Glossário

| Termo | Definição |
|---|---|
| **Saga** | Padrão para transações distribuídas: sequência de transações locais, cada uma com uma compensação que desfaz seu efeito em caso de falha |
| **Transação local** | Operação atômica dentro de um único serviço/banco — não envolve coordenação entre serviços |
| **Compensação** | Operação que desfaz semanticamente o efeito de uma transação anterior. Não é um rollback técnico — é uma nova transação com lógica inversa |
| **Saga Orquestrada** | Variação do padrão onde um componente central (Orchestrator) comanda cada etapa e suas compensações |
| **Idempotência** | Propriedade de uma operação que produz o mesmo resultado independentemente de quantas vezes for executada com os mesmos parâmetros |
| **`signup_attempt_id`** | UUID gerado pelo frontend ao carregar o formulário, usado para identificar uma tentativa de cadastro e permitir retries seguros |
| **CDC** | Change Data Capture — captura eventos de mudança diretamente do log do banco de dados, sem polling |
| **Strangler Fig** | Padrão de migração incremental onde o novo sistema é construído ao redor do antigo até substituí-lo completamente |
| **Ponto de não retorno** | Momento da Saga após o qual a operação é considerada commitada e não há mais compensação possível |
