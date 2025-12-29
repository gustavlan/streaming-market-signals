# Music Industry Alternative Data & Price Signals

End-to-end alternative data pipeline that constructs a proprietary dataset tracking the daily streaming market share of major music labels (Universal, Sony, Warner) and correlates this with their stock prices for quantitative research.

```mermaid
flowchart LR
    A["Spotify Charts\n(Alt Data)"] --> B["Label Enrichment"] --> C["Entity Resolution\n(Graph Theory)"] --> D["Market Share\nTime Series"] --> E["Stock Correlation\n& Alpha Signals"]
```

## Results

Market share by parent group (Dec 2024 daily charts, 26.9B streams):

| Parent Group | Streams | Share |
|-------------|--------:|------:|
| Universal Music Group | 11.3B | 41.9% |
| Sony Music Entertainment | 6.3B | 23.3% |
| Warner Music Group | 5.4B | 20.2% |
| Independent / Other | 2.9B | 10.8% |
| Unmapped | 1.0B | 3.8% |
| **Big 3 Total** | **23.0B** | **85.4%** |

The 85% reported here is higher than the commonly cited 65-70% for the big 3 because this project measures chart-topping tracks only, hits skew heavily toward majors who dominate playlist placement and marketing. The 65-70% figure covers all streaming including long-tail catalog.

## Architecture

Daily charts are scraped from Kworb via Playwright, enriched with label metadata from Spotify's API, then joined against MusicBrainz's 321K label taxonomy. dbt transforms resolve ownership hierarchies and aggregate streams into an incremental fact table by parent group.

**Stack:** Docker Compose · Airflow 2.10 · DuckDB · dbt-core · NetworkX

```mermaid
flowchart LR
  subgraph Sources
    KW["Kworb charts"];
    MB["MusicBrainz dump"];
    SP["Spotify API"];
    YF["Yahoo Finance"];
  end

  subgraph Airflow
    SETUP["setup_musicbrainz_data"];
    INGEST["music_market_share_ingest"];
    PIPE["music_market_share_pipeline"];
    FIN["music_financials_daily"];
    INGEST --> PIPE;
  end

  subgraph Warehouse
    DUCK[("DuckDB: music_warehouse.duckdb")];
    RAW["Raw tables (ingested)"];
    STG["Staging (dbt)"];
    INT["Intermediate (dbt)"];
    MART["Mart: fact_market_share"];
    DUCK --> RAW --> STG --> INT --> MART;
    DIM["dim_labels"];
  end

  subgraph Analytics
    NB["market_share_alpha.ipynb (signals + correlation)"];
  end

  KW --> INGEST --> DUCK;
  MB --> SETUP --> DUCK;
  SP --> PIPE --> DUCK;
  YF --> FIN --> DUCK;

  SETUP -. builds .-> DIM;
  DIM -. used in transforms .-> INT;

  MART --> NB;
  DUCK --> NB;
```


## Entity Resolution via Graph Theory

A key challenge is resolving the ultimate parent company to a tradable entity for thousands of sub-labels. For example: "pgLang, under exclusive license to Interscope Records" → Interscope → Universal Music Group. The `setup_musicbrainz_data` workflow produces the label hierarchy dimension (`dim_labels`).

The [`scripts/build_hierarchy.py`](scripts/build_hierarchy.py) script uses NetworkX to:
1. Build a directed graph of 64K+ label-to-label ownership relationships from MusicBrainz
2. Traverse the graph recursively to find the root parent node for each label
3. Classify labels into market share groups (UMG, Sony, Warner, Independent)

This approach handles complex nested ownership that would be impossible to resolve with simple pattern matching.

## Quantitative Analysis

The [`notebooks/market_share_alpha.ipynb`](notebooks/market_share_alpha.ipynb) notebook explores: Market share trends of the big 3 with moving averages. Momentum signals, Z-score deviations, rate-of-change indicators. Stock Correlation,Market share vs UMG.AS, WMG, SONY stock prices.Lead/Lag Analysis, does streaming momentum predict stock returns?


## Quickstart

```bash
cp .env.example .env  # Add Spotify credentials for enrichment
docker compose up --build
```

Open Airflow at `localhost:8080` (admin/admin). Trigger `setup_musicbrainz_data`, then unpause `music_market_share_ingest`.

Query results:
```bash
duckdb data/music_warehouse.duckdb "
  SELECT parent_group, SUM(total_streams) as streams
  FROM fact_market_share GROUP BY 1 ORDER BY 2 DESC
"
```

## Data Sources

| Source | Description | Update |
|--------|-------------|--------|
| [Kworb](https://kworb.net) | Daily Spotify global charts | Daily scrape |
| [MusicBrainz](https://musicbrainz.org) | Label ownership relationships | Manual dump load |
| Spotify API | Track → album → label metadata | On-demand enrichment |
| Yahoo Finance | UMG.AS, WMG, SONY stock prices | On-demand fetch |

## Future Work

- Merlin integration — Cross-reference indie labels against Merlin member list to distinguish independent from indie distributed by major.
- Backtesting framework — Build simple trading strategies based on market share momentum signals
- Granger causality — Statistical tests to validate predictive relationships
- Earnings correlation — Map quarterly market share trends to earnings surprises
- Multi-platform expansion — Add Apple Music, Amazon Music for broader coverage

## License

MIT
