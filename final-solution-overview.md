# Visão Geral da Solução Final

## Visão Geral

Este projeto propõe a extração do fluxo de cadastro de usuários de um sistema monolítico legado para uma arquitetura distribuída baseada em microserviços, focada em escalabilidade, resiliência, consistência e migração incremental sem downtime.

O monólito atual sofre com contenção de recursos, gargalos de infraestrutura compartilhada e instabilidade operacional causada por mais de 50 serviços executando no mesmo processo e compartilhando o mesmo banco Oracle.

A solução proposta remove a dependência síncrona entre o fluxo de cadastro e o monólito, preservando a compatibilidade com o ecossistema legado durante toda a migração.

---

# Objetivos

A solução busca:

* Desacoplar o cadastro de usuários do monólito
* Remover a dependência síncrona do banco Oracle
* Melhorar disponibilidade e escalabilidade
* Preservar a performance do monólito durante a migração
* Suportar cadastro + criação de assinatura de forma síncrona
* Garantir consistência entre serviços sem utilizar transações distribuídas (2PC)
* Permitir retries seguros sem duplicidade de usuários
* Possibilitar rollout gradual com suporte a rollback

---

# Arquitetura Proposta

A nova arquitetura introduz:

* **Signup Frontend**

  * Nova interface de cadastro
  * Geração do `signup_attempt_id`

* **Signup Orchestrator**

  * Orquestração centralizada da Saga
  * Controle de retries e idempotência
  * Coordenação das operações distribuídas

* **User Service**

  * Banco PostgreSQL próprio
  * Gerenciamento do ciclo de vida do usuário
  * Validação de unicidade do email

* **Subscription Service**

  * Serviço já existente fora do monólito
  * Responsável pela criação da assinatura

* **User Sync Worker**

  * Sincronização baseada em CDC
  * Replicação Oracle ↔ PostgreSQL
  * Manutenção de projeções locais para o monólito

---

# Consistência Distribuída

Para garantir consistência entre a criação do usuário e da assinatura, a solução utiliza o padrão **Saga Orquestrada**.

## Cadastro sem assinatura

1. Usuário envia o formulário
2. User Service cria o usuário diretamente como `ACTIVE`
3. Operação finaliza com sucesso

## Cadastro com assinatura

1. Usuário é criado como `PENDING_SUBSCRIPTION`
2. Subscription Service cria a assinatura
3. Usuário é promovido para `ACTIVE`

Caso qualquer etapa falhe:

* ações de compensação são executadas
* o usuário é marcado como `CANCELLED`
* o retry permanece possível utilizando o mesmo email

Isso garante que o usuário nunca perceba estados inconsistentes.

---

# Estratégia de Retry e Idempotência

O principal desafio técnico é evitar o problema de “email já cadastrado” após falhas parciais.

A solução introduz:

* `signup_attempt_id`
* rastreamento centralizado das tentativas
* estados do ciclo de vida do usuário
* lógica de compensação da Saga

Isso permite:

* retries seguros
* prevenção de duplicidade
* recuperação após timeout ou falhas parciais
* comportamento distribuído determinístico

---

# Estratégia de Migração

A migração segue o padrão **Strangler Fig**:

1. Introduzir os novos serviços paralelamente ao monólito
2. Replicar usuários via CDC
3. Redirecionar tráfego gradualmente
4. Manter projeções Oracle para preservar JOINs legados
5. Remover dependências de forma incremental

Essa estratégia reduz riscos operacionais e evita downtime.

---

# Compatibilidade com o Monólito

Para evitar degradação de performance no sistema legado:

* nenhuma chamada HTTP síncrona é adicionada às queries existentes
* projeções locais no Oracle preservam os JOINs atuais
* foreign keys permanecem intactas durante a migração
* o rollout ocorre gradualmente utilizando feature flags e A/B testing

---

# Principais Decisões Arquiteturais

| Decisão                              | Motivo                                |
| ------------------------------------ | ------------------------------------- |
| Saga Pattern                         | Consistência distribuída sem 2PC      |
| Saga Orquestrada                     | Controle centralizado e retries       |
| PostgreSQL no User Service           | Escalabilidade independente           |
| Sincronização via CDC                | Migração sem downtime                 |
| Projeções locais no Oracle           | Preservar performance do monólito     |
| Idempotência com `signup_attempt_id` | Retries seguros e tolerância a falhas |

---

# Benefícios Esperados

* Maior disponibilidade do cadastro
* Menor dependência do monólito
* Melhor escalabilidade
* Operações distribuídas mais seguras
* Modernização incremental
* Melhor isolamento de falhas
* Maior produtividade dos times
* Menor risco operacional durante a migração

---

# Conclusão

A solução proposta moderniza o fluxo de cadastro através de uma arquitetura resiliente baseada em microserviços, capaz de evoluir independentemente do monólito.

Combinando Saga Orquestrada, retries idempotentes, sincronização via CDC e estratégias de migração incremental, a arquitetura alcança consistência distribuída preservando estabilidade do sistema e experiência do usuário durante toda a transição.
