# Extração de Cadastro — Monólito para Microserviços

Este projeto apresenta uma proposta arquitetural para extrair o fluxo de cadastro de usuários de um sistema monolítico para uma arquitetura distribuída baseada em microserviços.

A solução foca em:

- Remover a dependência síncrona do monólito e do banco Oracle
- Permitir um cadastro mais escalável e resiliente
- Garantir consistência distribuída utilizando Saga Pattern
- Tratar retries e idempotência com segurança
- Migrar dados incrementalmente sem downtime
- Preservar a performance do monólito durante a migração

## Documentação

- [Arquitetura da Solução](./register-architecture.md)
- [Fluxo Saga de Cadastro](./saga-register.md)

## Principais Conceitos

- Saga Orquestrada
- Idempotência com `signup_attempt_id`
- CDC (Change Data Capture)
- Migração incremental (Strangler Fig Pattern)
- Projeções locais para preservar JOINs do monólito
- Resiliência em sistemas distribuídos

## Objetivo

Definir uma estratégia de migração que permita evoluir o fluxo de cadastro de forma independente do monólito, mantendo confiabilidade, consistência e uma boa experiência para o usuário.
