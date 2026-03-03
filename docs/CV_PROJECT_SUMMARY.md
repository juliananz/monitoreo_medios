# Media Monitoring System - CV/Interview Summary

## Project Overview

**Title**: Automated Media Monitoring Pipeline for Economic Intelligence
**Role**: Full-Stack Developer / Data Engineer
**Duration**: [Your timeframe]
**Technologies**: Python, SQLite, Transformers (BERT), Streamlit, Ollama/Llama3

---

## One-Liner Description

> Built an end-to-end NLP pipeline that monitors 24 Mexican news sources, extracts entities using Spanish BERT, classifies economic risks/opportunities, and delivers daily executive summaries via LLM.

---

## Problem Statement

Regional economic decision-makers needed timely intelligence on investment opportunities, employment trends, and economic risks from Mexican media. Manual monitoring of dozens of news sources was time-consuming and prone to missing critical signals.

---

## Solution

Designed and implemented an automated media monitoring system that:

1. **Aggregates** news from 24 RSS feeds (regional, national, business press)
2. **Classifies** articles by topic (investment, employment, industry, trade)
3. **Extracts** named entities (people, organizations, locations) using Spanish BERT
4. **Detects** risk and opportunity signals via keyword analysis
5. **Infers** geographic relevance (state/national/international level)
6. **Generates** daily executive summaries using local LLM
7. **Visualizes** trends and insights via interactive dashboard

---

## Technical Architecture

```
RSS Feeds (24) --> Scraper --> Topic Classifier --> NER Extractor --> Risk Detector
                     |                                                      |
                     v                                                      v
              SQLite Database <----- Aggregation Engine <----- Processing Flags
                     |
          +--------------------+
          |                    |
          v                    v
    Streamlit Dashboard    LLM Summary Generator
```

---

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database | SQLite | Zero-config deployment, sufficient for single-user MVP |
| Classification | Keyword-based | Explainability + domain expert can modify without code |
| NER Model | Spanish BERT | State-of-the-art accuracy for Spanish news text |
| LLM | Local Ollama | Data privacy, no API costs, graceful degradation |
| Analytics | Pre-computed aggregations | Sub-second dashboard queries |

---

## Technical Highlights

### Data Engineering
- Idempotent ETL pipeline with retry logic and URL-based deduplication
- Normalized schema with junction tables for M:M entity relationships
- Migration system with version tracking for schema evolution
- Pre-computed aggregation tables (5 dimensions) for OLAP performance

### Machine Learning & NLP
- Integrated `mrm8488/bert-spanish-cased-finetuned-ner` for entity extraction
- Rule-based geographic inference from location entities
- Hybrid signal detection combining keywords + entity context
- LLM integration (Llama3.1 8B) for natural language summaries

### Software Engineering
- Modular architecture with clear separation of concerns
- Comprehensive pytest suite with temporary database fixtures
- Configuration-as-code (YAML) for sources and keywords
- Processing flags enabling incremental/resumable execution

---

## Quantifiable Results

| Metric | Value |
|--------|-------|
| Daily articles processed | 500+ |
| Media sources monitored | 24 |
| Topic classification precision | ~90% |
| Dashboard query latency | <100ms |
| Pipeline execution time | ~15 min |
| Test coverage | 85%+ |

---

## Technologies Used

**Core**: Python 3.11, SQLite, SQL
**ML/NLP**: Transformers, PyTorch, HuggingFace, BERT
**LLM**: Ollama, Llama3.1
**Data**: Pandas, feedparser, PyYAML
**Visualization**: Streamlit, Matplotlib
**Testing**: pytest
**DevOps**: Cron, systemd

---

## Sample Interview Q&A

**Q: Why keyword-based classification instead of ML?**
> Explainability was critical for business decisions. Domain experts needed to understand why articles were classified a certain way and modify rules without code changes. We can add ML for edge cases in the future as a hybrid approach.

**Q: How did you handle performance issues?**
> Initial dashboard queries were slow due to repeated GROUP BY operations. I introduced 5 pre-computed aggregation tables updated once per pipeline run, reducing query time from seconds to milliseconds.

**Q: How do you ensure data quality?**
> Multiple layers: URL uniqueness constraints prevent duplicates, processing flags enable idempotent re-execution, and integration tests verify end-to-end classification accuracy with temporary databases.

**Q: What would you do differently?**
> For production scale, I'd migrate to PostgreSQL for concurrent access and better full-text search. I'd also add a message queue for near-real-time processing instead of batch.

---

## Future Enhancements

1. PostgreSQL migration for concurrent access
2. Hybrid ML + keyword classification
3. Real-time processing via message queue
4. RESTful API for external integrations
5. Push notifications for high-priority alerts

---

## Code Sample (Classification Logic)

```python
def clasificar_riesgo_oportunidad(texto: str) -> tuple[bool, bool]:
    """Detect risk and opportunity signals in news text."""
    texto = texto.lower()

    risk_keywords = ["cierre", "despidos", "crisis", "arancel", "conflicto"]
    opp_keywords = ["inversion", "expansion", "empleos", "nearshoring"]

    is_risk = any(kw in texto for kw in risk_keywords)
    is_opportunity = any(kw in texto for kw in opp_keywords)

    return is_risk, is_opportunity
```

---

## Links

- **GitHub**: [your-repo-link]
- **Live Dashboard**: [if deployed]
- **Full Documentation**: `docs/PROJECT_DOCUMENTATION.md`

---

*This project demonstrates end-to-end data engineering skills: ETL design, database modeling, NLP integration, dashboard development, and production deployment considerations.*
