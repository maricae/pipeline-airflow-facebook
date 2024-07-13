# Apache Airflow Pipeline - API Facebook Marketing

Este projeto apresenta uma data pipeline estruturada no Apache Airflow do consumo de dados da API Facebook Marketing com informações das campanhas (Meta ADS). Os dados foram inseridos em um banco de dados MySQL para análise do time de marketing.

## API de Marketing
https://developers.facebook.com/docs/marketing-apis/

## Ad Set Insights
https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/insights

## Consumo de Dados
Com o objetivo em analisar o andamento das campanhas, busquei apenas os conjuntos de anúncio que estavam no status de ATIVO. As variáveis selecionadas foram:

| Variáveis | Descrição |
| --- | --- |
| `adset_id` | ID do conjunto de anúncios |
| `adset_name` | Nome do conjunto de anúncios |
| `campaign_id` | ID da Campanha |
| `campaign_name` | Nome da Campanha |
| `spend` | Gasto total com o conjunto de anúncios |
| `objective` | Objetivos da campanha |
| `date_start` | Data de início |
| `date_stop` | Data de fim |
| `status` | Status do conjunto de anúncios |
| `created_time` | Data de criação do conjunto de anúncios |
| `leads` | Número de leads |

A tabela de retorno é agrupada por conjunto de anúncios (adset). Lembrando que anúncios fazem parte de um conjunto de anúncios (adset) e os adsets fazem parte de uma campanha.

## Etapas
### 1. Request Adset
Busca informações de adsets ativos.
### 2. Request Content 
Busca as variáveis de insights referente a adsets ativos.
### 3. Request Actions
Busca o número de Leads.
### 4. Truncate Table
Limpa a tabela do banco a entrada dos novos dados atualizados.
### 5. Insert Table
Insere os dados atualizados.
### 6. Email on Failed
Envia e-mail caso houver falha em algumas das etapas.
