# Big Data Project
# Gut‑Brain Organoid Data Warehouse

Build reproducible, production‑grade pipelines to transform raw multi‑omics organoid experiments into actionable insights on the gut–brain axis.

---

## 🌐 Why a Data Warehouse for Organoids?

### Theoretical Motivation

* **Gut–brain axis**: Microbiome‑derived molecules (short‑chain fatty acids, cytokines, neurotransmitter mimetics) influence neuro‑immune signalling.
* **Organoids**: Lab‑grown mini‑tissues recapitulate structure/function of brain & gut while enabling controlled perturbations.
* **Data deluge**: Each RNA‑seq run ships as isolated FASTQ + MAGE‑TAB bundles; proteomics adds another layer.
* **Warehouse advantage**: A dimensional model unifies experiments and makes cross‑study queries trivial—fuel for biomarker discovery and ML.

### Intuition

> “If I expose cell‑type *X* to metabolite *Y*, which genes light up and what does that say about neuro‑inflammation?”

The **live database** records the vocabulary (Stimuli, Microbes, Genes), while the **warehouse** restates it in a *star schema* so analysts can slice by stimulus, cell type, or disease context in real‑time.

---

## 🏗️ Architecture in a Nutshell

```mermaid
graph LR
  subgraph OLTP (MySQL Live DB)
    A[Samples] -->|triggers| B[ExpressionStats]
    A --> C[Stimuli]
    C --> D[Microbes]
  end
  subgraph Airflow ETL
    E[Incremental Extract] --> F[Staging]
    F --> G[Dimensional Load]
  end
  subgraph OLAP (Snowflake‑style DW)
    G --> H[Fact_Expression]
    G --> I[Dim_Gene]
    G --> J[Dim_Stimulus]
    G --> K[Dim_CellType]
  end
  H --> L[PowerBI Dashboards]
```

* **Integrity**: Foreign keys, triggers (e.g., auto‑recompute significance), and scheduled **EVENTS** safeguard OLTP.
* **Orchestration**: Apache Airflow DAGs ingest GEO / ArrayExpress, harmonise ontologies, and populate the DW.
* **BI**: Power BI connects to OLAP for interactive dashboards.

---

## 🔧 Tech Stack

| Layer                     | Technology                                           |
| ------------------------- | ---------------------------------------------------- |
| **OLTP**                  | MySQL 8.x                                            |
| **Orchestration**         | Apache Airflow 2.x                                   |
| **Analytics & ML**        | Python 3.11 (Anaconda, pandas, scikit‑learn, scanpy) |
| **Business Intelligence** | Power BI Desktop / Service                           |
| **DevOps**                | Docker, Kubernetes, GitHub Actions, pre‑commit       |

---

## 🚀 Quick Start

1. **Clone & conda‑env**

   ```bash
   git clone https://github.com/<you>/gut‑brain‑dw.git
   cd gut‑brain‑dw
   conda env create -f environment.yml
   conda activate gutbrain
   ```
2. **Spin‑up MySQL**

   ```bash
   docker compose up mysql
   mysql -u root -p < database/schema_live.sql
   ```
3. **Initialise Airflow**

   ```bash
   docker compose up airflow  # UI at http://localhost:8080
   ```
4. **Seed sample dataset (E‑MTAB‑11468)**

   ```bash
   python src/etl/load_geo.py --accession E-MTAB-11468
   ```
5. **Run the ETL DAG** – watch staging & fact tables populate.
6. **Open PowerBI dashboard** in `dashboards/GutBrain.pbix` and hit *Refresh*.

---

## 🧠 ML & Analytical Recipes

| Question                                                          | Notebook                                |
| ----------------------------------------------------------------- | --------------------------------------- |
| Metabolites that up‑regulate **STAT3** in astrocytes              | `notebooks/stat3_screen.ipynb`          |
| Predicting **High** significance hits from microbe–stimulus pairs | `notebooks/random_forest_microbe.ipynb` |
| Co‑culture vs monoculture endothelial response to TNF‑α           | `notebooks/umap_tnf_endothelial.ipynb`  |


---

## 📊 Dashboard Highlights

| Tile               | Insight                                               |
| ------------------ | ----------------------------------------------------- |
| **Volcano filter** | interactive gene selection by log₂FC & FDR            |
| **Sankey**         | microbe → metabolite → cell‑type impact flow          |
| **Heatmap**        | organoid vs in‑vivo tissue similarity                 |
| **Timeline**       | rolling average of high‑significance hits per pathway |

---

## 🔒 Data Governance & Safety Nets

* **Row‑level triggers** prevent orphaned stats and downgrade p‑values > 0.05.
* **Audit tables** log every ETL batch (checksum, source, duration).
* **GRANTS** restrict writes; ML jobs hit read‑only replicas.

---

## 🛣️ Roadmap

* [ ] Ontology auto‑updates (EFO, Uberon).
* [ ] CI/CD to publish PowerBI template via REST API.
* [ ] Integrate proteomic (SWATH‑MS) support.

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---

## 📜 License

MIT — see `LICENSE` for details.

---

## 🙏 Acknowledgements

Course project for *Big Data & Warehousing* (2025).
Thanks to ArrayExpress, Human Organoid Atlas, and Vanderbilt VANTAGE for open data.
