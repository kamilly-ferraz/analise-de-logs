# Data Lake de Logs - Nerbos

## 🔹 Descrição do Projeto
Este projeto simula um **Data Lake de logs de acesso de usuários** em **PostgreSQL**, permitindo práticas de engenharia de dados como **particionamento**, **ETL** e **análise de dados**.  

O pipeline gera dados fictícios de acesso de usuários (com IP, página visitada, status e tempo de resposta), cria tabelas particionadas por mês no PostgreSQL e realiza análises rápidas de métricas chave.

---

## 🔹 Funcionalidades do Projeto

1. **Testa conexão com PostgreSQL:**  
   - Verifica se é possível conectar ao banco local.

2. **Geração de logs falsos:**  
   - Cria um conjunto de dados sintético com 100.000 linhas (ou mais, configurável);  
   - Inclui informações como:
     - `timestamp` → momento do acesso  
     - `user_id` → ID do usuário (UUID)  
     - `ip` → endereço IP  
     - `pagina` → página acessada (home, produto, login, carrinho, checkout)  
     - `status` → código HTTP (200, 404, 500, 301)  
     - `tempo_resposta` → tempo em segundos  
   - Cria coluna auxiliar `data` (somente a data do acesso) para particionamento.

3. **Criação do Data Lake particionado:**  
   - Cria tabela principal `logs_acesso` particionada por **range de datas (mensal)**;  
   - Cria automaticamente tabelas particionadas do tipo `logs_YYYY_MM`;  
   - Insere os dados gerados nas partições corretas.

4. **Análise do Data Lake:**  
   - Total de acessos por página;  
   - Top 3 dias com mais erros 500;  
   - Listagem de todas as partições criadas.

---

## 🔹 Tecnologias Utilizadas
- **Python 3.x** – manipulação e automação de dados  
- **pandas** – geração e transformação de dados  
- **Faker** – geração de dados sintéticos  
- **PostgreSQL** – Data Lake particionado  
- **psycopg2** – conexão e carga de dados no PostgreSQL

  <img width="474" height="247" alt="Captura de Tela 2025-11-15 às 17 29 08" src="https://github.com/user-attachments/assets/5adef744-170b-4d58-9877-2c9115f039b0" />
