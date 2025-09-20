# BCH — dbt Project (Staging + Data Mart) with CI
## Goal
- Staging: materialize Bitcoin Cash transactions for the last ~3 months (90 days from the latest available block), deduplicated by `hash` → `tx_hash`.

- Data Mart: materialize the current balance per address (sum of UTXOs), excluding addresses that participated in coinbase transactions.

- Quality: tests on staging and mart.

- Docs: dbt’s documentation site generated.

- CI: GitHub Actions runs dbt on PR create/update, authenticating to GCP via Workload Identity Federation (WIF).


## 0) Prerequisites
GCP:

- Project: `astrafy-challenge-sonia-001` (or yours).

- BigQuery in US.

- Service Account for CI (e.g., `dbt-ci-sa@<PROJECT_ID>.iam.gserviceaccount.com`) with:
  - `roles/bigquery.jobUser` (project level).

  - `roles/bigquery.dataEditor` on datasets staging and data_mart.

Workload Identity Federation (WIF) configured:

- Pool: `github-pool`

- Provider: `github-provider`

- Attribute mapping includes `attribute.repository`.

- Attribute condition (recommended): `attribute.repository=="<Owner>/<Repo>"`.

- Binding to the SA: `roles/iam.workloadIdentityUser` with
`principalSet://.../workloadIdentityPools/github-pool/attribute.repository/<Owner>/<Repo>`.

GitHub repo: `Owner/Repo` (e.g., `Soniaremacha/bch-dbt`).

## 1) Project structure

```bash
.
├─ dbt_project.yml
├─ models/
│  ├─ sources/
│  │  └─ bch_sources.yml
│  ├─ staging/
│  │  ├─ stg_bch_transactions_last_3m.sql
│  │  └─ schema.yml
│  └─ marts/
│     ├─ address_current_balance.sql
│     └─ schema.yml
├─ macros/
│  └─ generate_schema_name.sql      # (optional) avoid duplicate schema names
├─ docs/
│  └─ overview.md                   # (optional) reusable doc block
├─ requirements.txt                 # Python deps (dbt-bigquery)
└─ .github/workflows/dbt-pr.yml     # CI on PRs

```
For local dev, use a Python 3.10+ virtualenv.

## 2) dbt configuration
### 2.1 Install dependencies
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt           # contains: dbt-bigquery>=1.6,<2.0
```

### 2.2 `profiles.yml`
On your machine: `~/.dbt/profiles.yml` (in CI it’s created inside the workflow).
```yaml
bch_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth           # local: use gcloud auth application-default login
      project: astrafy-challenge-sonia-001
      dataset: staging        # default; models can override schema
      location: US
      threads: 4
```
On Cloud Shell, `method: oauth` generally works out of the box.

### 2.3 dbt_project.yml
Ensure you have something like:
```yaml
name: 'bch_project'
version: '1.0.0'
config-version: 2

profile: 'bch_project'

model-paths: ['models']
macro-paths: ['macros']
seed-paths: ['seeds']
test-paths: ['tests']
docs-paths: ['docs']
analysis-paths: ['analysis']
target-path: 'target'
clean-targets: ['target', 'dbt_packages']

models:
  +materialized: table
```

## 3) Sources
`models/sources/bch_sources.yml`:

```yaml
version: 2

sources:
  - name: bch
    description: "Public Bitcoin Cash dataset in BigQuery."
    database: bigquery-public-data
    schema: crypto_bitcoin_cash

    loaded_at_field: block_timestamp
    freshness:
      warn_after: {count: 365, period: day}
      error_after: {count: 2000, period: day}

    tables:
      - name: transactions
        description: "One row per transaction; includes block_timestamp and is_coinbase."
        columns:
          - name: block_timestamp
            tests: [not_null]

      - name: outputs
        description: "Transaction outputs; value in satoshis, addresses (array)."
        columns:
          - name: transaction_hash
            tests: [not_null]
          - name: value
            tests: [not_null]
          - name: addresses
            tests: [not_null]

      - name: inputs
        description: "Transaction inputs; reference previous outputs via spent_*."
        columns:
          - name: spent_transaction_hash
          - name: spent_output_index

```
Note: we don’t test uniqueness of `hash` at the source level because there are historical duplicates. We enforce uniqueness in staging after dedupe.


## 4) Staging (last 90 days, deduped)

`models/staging/stg_bch_transactions_last_3m.sql`:
```sql
{{ config(
    materialized = 'table',
    schema = 'staging',
    alias = 'stg_bch_transactions_last_3m',
    partition_by = { "field": "block_timestamp", "data_type": "timestamp", "granularity": "day" }
) }}

with src as (
  select *
  from {{ source('bch','transactions') }}
),
maxd as (
  select max(block_timestamp) as max_ts
  from src
),
windowed as (
  select
    s.*,
    s.hash as tx_hash,
    row_number() over (
      partition by s.hash
      order by s.block_timestamp desc, s.hash
    ) as rn
  from src s
  cross join maxd m
  where s.block_timestamp >= timestamp_sub(m.max_ts, interval 90 day)  -- ~3 months
)
select * except(rn)
from windowed
where rn = 1
```

`models/staging/schema.yml`:

```yaml
version: 2
models:
  - name: stg_bch_transactions_last_3m
    description: "Transactions in the last ~3 months (rolling from max timestamp), deduped by hash."
    columns:
      - name: tx_hash
        tests: [not_null, unique]
      - name: block_timestamp
        tests: [not_null]

```


## 5) Data Mart (current balance per address, excluding coinbase)
`models/marts/address_current_balance.sql`:
```sql
{{ config(
    materialized='table',
    schema='data_mart',
    alias='address_current_balance'
) }}

-- 1) Addresses that ever appeared in coinbase txs
with coinbase_addrs as (
  select distinct o_address as address
  from (
    select
      o.transaction_hash,
      o.value,
      a as o_address
    from {{ source('bch','outputs') }} o, unnest(o.addresses) as a
  ) o
  join {{ source('bch','transactions') }} tx
    on tx.hash = o.transaction_hash
  where tx.is_coinbase = true
),

-- 2) Spent outputs (inputs reference previous outputs)
spent_outputs as (
  select
    i.spent_transaction_hash as tx_hash,
    i.spent_output_index     as out_index
  from {{ source('bch','inputs') }} i
  where i.spent_transaction_hash is not null
),

-- 3) Current UTXOs = outputs with no match in spent_outputs
unspent_outputs as (
  select
    a as address,
    o.value
  from {{ source('bch','outputs') }} o, unnest(o.addresses) as a
  left join spent_outputs s
    on s.tx_hash   = o.transaction_hash
   and s.out_index = o.index
  where s.tx_hash is null
),

-- 4) Balance per address (satoshis → BCH)
balances as (
  select
    address,
    sum(value) as balance_sats
  from unspent_outputs
  group by 1
)

select
  b.address,
  b.balance_sats / 1e8 as balance_bch
from balances b
left join coinbase_addrs c using (address)
where c.address is null

```

`models/marts/schema.yml`:
```yaml
version: 2
models:
  - name: address_current_balance
    description: "Current balance per address (sum of UTXOs), excluding addresses with any coinbase tx."
    columns:
      - name: address
        tests: [not_null, unique]
      - name: balance_bch
        tests: [not_null]

```


## 6) Documentation (dbt docs)
Generate and serve docs:
```bash
dbt docs generate
dbt docs serve --port 8080 --no-browser

```
On Cloud Shell → Web Preview → Port 8080.


## 7) Run
```bash
dbt debug
dbt run  -s stg_bch_transactions_last_3m address_current_balance
dbt test -s stg_bch_transactions_last_3m address_current_balance

```

## 8) CI with GitHub Actions (WIF to GCP)
### 8.1 Repo secrets (GitHub → Settings → Secrets and variables → Actions)

- `GCP_WIF_PROVIDER` = `projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-pool/providers/github-provider`

- `GCP_SA_EMAIL` = `dbt-ci-sa@<PROJECT_ID>.iam.gserviceaccount.com`


### 8.2 Workflow `.github/workflows/dbt-pr.yml`
```yaml
name: dbt run on Pull Requests

on:
  pull_request:
    branches: [ main, master ]
    types: [opened, synchronize, reopened]
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

env:
  DBT_PROFILES_DIR: ./.github/profiles

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Authenticate to Google Cloud (WIF)
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SA_EMAIL }}
          project_id: <PROJECT_ID>

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"
          cache-dependency-path: "requirements.txt"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Create dbt profile
        run: |
          mkdir -p .github/profiles
          cat > .github/profiles/profiles.yml <<'PROF'
          bch_project:
            target: ci
            outputs:
              ci:
                type: bigquery
                method: oauth
                project: <PROJECT_ID>
                dataset: staging
                location: US
                threads: 4
          PROF

      - name: dbt deps
        run: dbt deps --project-dir .

      - name: dbt build (models + tests)
        run: dbt build --project-dir . -s stg_bch_transactions_last_3m address_current_balance --fail-fast

```

## 9) Troubleshooting

- setup-python: “No file matched requirements.txt”
  → Add `requirements.txt` and set `cache-dependency-path`.

- auth@v2: “workload_identity_provider or credentials_json”
  → Create both secrets (`GCP_WIF_PROVIDER`, `GCP_SA_EMAIL`). PR must be from the same repo (not a fork).

-Invalid principalSet member when binding
  → Use the pool FQN (not the provider) in
  `principalSet://.../workloadIdentityPools/<POOL_ID>/attribute.repository/<Owner>/<Repo>`.

- High BigQuery costs

  - Avoid wrapping partition columns (e.g., `DATE(col)`); compare TIMESTAMP directly.

  - Set `partition_by` and optionally `cluster_by`.

  - Select only required columns; avoid `SELECT *`.
 

## 10) Quick checks in BigQuery
```sql
-- Staging window sanity
SELECT MIN(DATE(block_timestamp)) AS min_d,
       MAX(DATE(block_timestamp)) AS max_d,
       COUNT(*) AS n
FROM `<PROJECT_ID>.staging.stg_bch_transactions_last_3m`;

-- Data mart: count and % with balance > 0
SELECT COUNT(*) AS total_rows,
       SAFE_DIVIDE(COUNTIF(balance_bch > 0), COUNT(*)) AS pct_with_balance
FROM `<PROJECT_ID>.data_mart.address_current_balance`;

```

## 11) Key decisions (recap)
- Rolling window (90 days) anchored at `MAX(block_timestamp)` to avoid empty recency gaps.

- Dedup by `hash` in staging with `row_number()`, exposing `tx_hash`.

- Exclude coinbase via `transactions.is_coinbase = true`.

- Quality: strict tests in staging and mart; source only uses `not_null` on safe fields + freshness.


All set! With this, you can install, run, test, document, and CI the dbt project end-to-end.
