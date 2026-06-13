# Hermetic Data Pipeline

> 문서 기반 RAG 챗봇을 위한 종단간 데이터 파이프라인 — Scrapy → Airflow → PySpark → Iceberg → pgvector → FastAPI

---

## 📋 프로젝트 개요

Hermetic Data Pipeline은 **폐쇄망 환경에서 운영 가능한 대용량 데이터 파이프라인**입니다. 조직 내 위키·매뉴얼 등의 문서를 수집하여 데이터 레이크하우스에 적재하고, 임베딩 벡터로 변환한 뒤 RAG(Retrieval-Augmented Generation) 방식으로 AI 챗봇이 질문에 답변할 수 있도록 하는 전 과정을 17개 모듈로 구성했습니다.

**주요 특징:**
- Apache Iceberg 기반 타임트래블·스키마 진화·컴팩션 지원
- pgvector 기반 유사도 검색 (IVFFlat / HNSW 인덱싱)
- Airflow DAG을 통한 증분 적재 및 재시도 자동화
- FastAPI SSE 스트리밍 응답 (StreamingResponse)
- 폐쇄망 배포 키트 (pip 오프라인 팩, Docker 이미지 export, Kerberos 인증)

## 🗺️ 시스템 아키텍처

아키텍처 다이어그램은 `docs/architecture.html`을 브라우저로 열면 확인할 수 있습니다.

```
📁 docs/architecture.html   ← 브라우저에서 열면 SVG 아키텍처 다이어그램
```

**데이터 흐름 요약:**

```
[내부 위키] → [Scrapy 크롤러] → [Airflow] → [PySpark] → [Iceberg Lakehouse]
                                                                ↓ 벡터화
[사용자] ← [Chat UI] ← [Private LLM] ← [FastAPI RAG] ← [pgvector]
```

## 🧱 기술 스택

| Category | Technologies |
|----------|-------------|
| **Orchestration** | Apache Airflow 2.8 (LocalExecutor), SparkSubmitOperator |
| **Processing** | PySpark 3.3, Apache Iceberg (HiveMetastore catalog) |
| **Storage** | Apache Iceberg (Parquet), PostgreSQL 15 + pgvector |
| **API / Serving** | FastAPI, LangChain, Pydantic v2, asyncpg |
| **Ingestion** | Scrapy, httpx (async), CDC (updated_at 기반) |
| **Infrastructure** | Docker Compose, Spark Standalone Cluster |
| **Deploy** | Offline pip packages, Docker save/load, Kerberos |

## 📦 프로젝트 구조

```
hermetic-data-pipeline/
├── dags/                    # Airflow DAGs
│   ├── postgres_incremental_dag.py    # PostgreSQL 증분 적재
│   ├── xcom_optimization_dag.py       # XCom 경량화 패턴
│   └── spark_iceberg_merge_dag.py     # Spark MERGE INTO DAG
├── spark/jobs/              # PySpark + Iceberg jobs
│   ├── iceberg_schema_evolution.py    # 스키마 진화
│   ├── time_travel_recovery.py        # 타임트래블/복구
│   ├── compaction_optimization.py     # Small files 컴팩션
│   └── hive_to_iceberg_migration.py   # Hive → Iceberg 마이그레이션
├── api/                     # FastAPI RAG 서버
│   ├── app/
│   │   ├── main.py                    # 앱 진입점, CORS, 헬스체크
│   │   ├── core/config.py             # Pydantic BaseSettings
│   │   ├── models/schemas.py          # ChatRequest/ChatResponse
│   │   ├── routers/chat.py            # /chat, /chat/stream 엔드포인트
│   │   └── services/
│   │       ├── vector_store.py        # pgvector CRUD + 유사도 검색
│   │       └── rag_chain.py           # LangChain RAG 파이프라인
│   ├── Dockerfile
│   └── requirements.txt
├── crawler/                 # Scrapy 크롤러
│   ├── scrapy_spider/spiders/wiki_spider.py
│   ├── scrapy.cfg
│   └── Dockerfile
├── ingestion/               # CDC 데이터 수집
│   ├── async_collector.py            # 비동기 HTTP 수집
│   └── dedup_processor.py            # 중복 제거/멱등성 처리
├── deploy/                  # 폐쇄망 배포 키트
│   ├── offline_packages.sh           # pip 오프라인 패키징
│   ├── docker_export.sh              # Docker 이미지 export
│   ├── ssl_proxy_config.py           # 사내 SSL 프록시 대응
│   └── kerberos_setup.md             # Kerberos 인증 가이드
├── config/                  # 설정 파일
│   ├── airflow_connections.json
│   └── spark_defaults.conf
├── scripts/                 # 유틸리티
│   └── init_iceberg_catalog.py       # Iceberg 카탈로그 초기화
├── tests/                   # E2E 통합 테스트
│   ├── conftest.py
│   └── e2e_pipeline_test.py          # 전체 파이프라인 시뮬레이션
├── docs/
│   └── architecture.html             # 아키텍처 다이어그램
├── docker-compose.yml       # Airflow + Spark + PostgreSQL + Hive
├── pyproject.toml
└── .gitignore
```

## 🚀 빠른 시작

### 1. 클론 및 환경 설정

```bash
git clone https://github.com/bellaxtech/hermetic-data-pipeline.git
cd hermetic-data-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. 인프라 구동

```bash
docker compose up -d
```

서비스 포트:
- Airflow UI: `http://localhost:8080` (airflow/airflow)
- Spark Master UI: `http://localhost:8081`
- Spark Worker UI: `http://localhost:8082`
- PostgreSQL: `localhost:5432`
- Hive Metastore Thrift: `localhost:9083`

### 3. Iceberg 카탈로그 초기화

```bash
python scripts/init_iceberg_catalog.py
```

### 4. Airflow DAG 활성화

Airflow UI에서 다음 DAG을 unpause:
- `postgres_incremental_load` — 매일 PostgreSQL 증분 적재
- `xcom_optimization_demo` — XCom 경량화 패턴 데모
- `spark_iceberg_merge` — Spark MERGE INTO 배치

### 5. API 서버 실행

```bash
cd api
pip install -r requirements.txt
PYTHONPATH=. uvicorn app.main:app --reload --port 8000
```

실행 점검(모듈 충돌 확인):

```bash
cd api
../.venv/bin/python3 -c "import app; print(app.__file__)"
```

기준: 출력 값이 `/api/app/__init__.py` 이어야 합니다.

진행 상황 추적이 필요한 경우:

```bash
open docs/progress.html
```

### 6. RAG 쿼리 테스트

```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "보안 규정이 뭐야?"}'
```

## 🧪 모듈 구성

| Part | 모듈 | 내용 | 시간 |
|------|------|------|------|
| **1부: Lakehouse** | 1~7 | Airflow + PySpark + Iceberg 인프라, 스키마 진화, 타임트래블, 컴팩션, 증분 적재, Hive 마이그레이션 | 10h |
| **2부: CDC** | 8~10 | 비동기 수집, Dedup, Spark MERGE INTO DAG, Slack 알림 | 5.5h |
| **3부: AI RAG** | 11~13 | FastAPI 구조, pgvector 벡터 저장소, LangChain RAG 스트리밍 | 5h |
| **4부: 배포** | 14~17 | 폐쇄망 배포 키트, Scrapy 크롤러, E2E 통합, 문서화 | 6.5h |

## 📄 라이선스

Apache 2.0 License
