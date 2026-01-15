# E-commerce Real-time Data Pipeline

> 비정형 데이터(리뷰 텍스트, Nested JSON) + 실시간/배치 처리 + 클러스터 스케일링 데이터 파이프라인 포트폴리오 프로젝트

## 프로젝트 개요

이커머스 쇼핑몰의 실시간 이벤트 데이터를 수집, 처리, 분석하는 end-to-end 데이터 파이프라인입니다.

### 주요 특징

- **비정형 데이터 처리**: 리뷰 텍스트, Nested JSON 세션 이벤트
- **실시간 스트리밍**: Kafka → Spark Streaming → Iceberg
- **배치 분석**: Airflow 스케줄링, 일별 집계 및 리뷰 분석
- **성능 최적화**: 정렬 저장을 통한 쿼리 성능 개선 검증
- **클러스터 스케일링**: 트래픽 패턴에 따른 동적 스케일링

## 아키텍처

```
┌─────────────────────────┐
│   Data Generator        │
│   (Python)              │
│   - 쇼핑 이벤트         │
│   - 리뷰 텍스트         │
│   - 시간대별 트래픽     │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│        Kafka            │
│   - shopping-events     │
│   - reviews             │
│   - session-events      │
└───────────┬─────────────┘
            │
      ┌─────┴─────┐
      │           │
      ▼           ▼
┌───────────┐ ┌─────────────────┐
│  Spark    │ │  Spark Batch    │
│ Streaming │ │  (Airflow DAG)  │
└─────┬─────┘ └────────┬────────┘
      │                │
      ▼                ▼
┌─────────────────────────────┐
│      MinIO + Iceberg        │
│      (Data Lakehouse)       │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│          Grafana            │
│  - 실시간 매출 대시보드     │
│  - 클러스터 모니터링        │
└─────────────────────────────┘
```

## 기술 스택

| 기술 | 버전 | 역할 | 선택 이유 |
|------|------|------|-----------|
| **Apache Kafka** | 7.5.0 | 메시지 큐 | 실시간 스트리밍, 데이터 유실 방지 |
| **Apache Spark** | 3.5.0 | 데이터 처리 | Streaming/Batch 통합, 분산 처리 |
| **MinIO** | Latest | 오브젝트 스토리지 | HDFS NameNode SPOF 문제 회피, S3 호환 |
| **Apache Iceberg** | 1.4.2 | 테이블 포맷 | Data skipping으로 쿼리 성능 향상 |
| **Apache Airflow** | 2.8.0 | 워크플로우 관리 | DAG 기반 의존성 관리 |
| **Grafana** | Latest | 시각화 | 실시간 모니터링 대시보드 |
| **Docker** | - | 컨테이너화 | 재현 가능한 환경 |

### 기술 선택 이유

#### Hadoop 대신 MinIO를 선택한 이유

1. **NameNode SPOF 문제 회피**: HDFS는 NameNode가 단일 장애점(Single Point of Failure)
2. **Small Files Problem**: 작은 파일이 많을 때 NameNode 메모리 부족 문제
3. **S3 호환**: 클라우드 마이그레이션 용이

#### Hive 대신 Iceberg를 선택한 이유

1. **Data Skipping**: 파일 메타데이터를 활용한 불필요한 파일 스캔 방지
2. **Hidden Partitioning**: 파티션 구조 변경 없이 쿼리 최적화
3. **Time Travel**: 특정 시점 데이터 조회 가능
4. **Schema Evolution**: 스키마 변경이 용이

## 빠른 시작

### 1. 사전 요구사항

- Docker & Docker Compose
- 최소 16GB RAM, 4 CPU cores 권장

### 2. 클론 및 시작

```bash
# 프로젝트 클론
git clone <repository-url>
cd shopping-data-pipeline

# 권한 설정
chmod +x scripts/*.sh

# 파이프라인 시작
./scripts/start.sh
```

### 3. 서비스 접속

| 서비스 | URL | 계정 |
|--------|-----|------|
| Spark Master | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airflow | http://localhost:8090 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

### 4. 데이터 생성 시작

```bash
cd docker

# 일반 모드로 데이터 생성 시작
docker-compose --profile generator up -d data-generator

# 세일 모드 (10배 트래픽)
docker exec data-generator python main.py --mode sale
```

### 5. Spark Streaming Job 실행

```bash
./scripts/submit-streaming.sh shopping
./scripts/submit-streaming.sh reviews
./scripts/submit-streaming.sh sessions
```

### 6. 클러스터 스케일 업

```bash
cd docker

# 추가 워커 시작 (2, 3, 4번)
docker-compose --profile scale up -d
```

## 프로젝트 구조

```
shopping-data-pipeline/
├── docker/
│   ├── docker-compose.yml
│   ├── spark/
│   │   ├── Dockerfile
│   │   └── spark-defaults.conf
│   └── airflow/
│       └── Dockerfile
├── data-generator/
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── generators/
│       ├── shopping_event.py      # 쇼핑 이벤트 생성
│       ├── review.py              # 리뷰 텍스트 생성 (비정형)
│       ├── search_query.py        # 검색 쿼리 생성 (비정형)
│       ├── session_event.py       # Nested JSON 세션 (비정형)
│       └── traffic_pattern.py     # 시간대별 트래픽 패턴
├── spark-jobs/
│   ├── streaming/
│   │   ├── shopping_stream.py     # 쇼핑 이벤트 스트리밍
│   │   ├── reviews_stream.py      # 리뷰 스트리밍
│   │   ├── session_stream.py      # 세션 스트리밍
│   │   └── realtime_aggregation.py
│   └── batch/
│       ├── daily_aggregation.py   # 일별 집계
│       ├── review_analysis.py     # 리뷰 분석 (NLP)
│       ├── data_quality.py        # 데이터 품질 체크
│       └── performance_test.py    # 성능 테스트
├── airflow/
│   └── dags/
│       ├── daily_analytics_dag.py
│       └── performance_test_dag.py
├── grafana/
│   └── dashboards/
├── scripts/
│   ├── start.sh
│   ├── stop.sh
│   └── submit-streaming.sh
├── PROJECT_PLAN.md
└── README.md
```

## 데이터 스키마

### Shopping Events

```json
{
  "event_id": "ev_abc123",
  "event_type": "purchase",
  "user_id": "u_12345",
  "product_id": "pd_000001",
  "product_name": "나이키 운동화",
  "category": "fashion",
  "price": 120000,
  "timestamp": "2024-01-15T14:32:00Z",
  "device": {
    "type": "mobile",
    "os": "iOS",
    "app_version": "5.2.1"
  },
  "context": {
    "referrer": "instagram",
    "campaign": {"id": "camp_summer", "name": "여름 세일"}
  }
}
```

### Reviews (비정형 텍스트)

```json
{
  "review_id": "rv_xyz789",
  "product_id": "pd_000001",
  "rating": 5,
  "text": "배송이 정말 빨랐어요! 품질도 좋고 가격대비 만족합니다. 재구매 의사 있어요!",
  "images": ["img_001.jpg", "img_002.jpg"],
  "created_at": "2024-01-15T14:32:00Z"
}
```

### Session Events (Nested JSON)

```json
{
  "session_id": "sess_abc123",
  "user_id": "u_12345",
  "device": {
    "type": "mobile",
    "os": "iOS",
    "screen": {"width": 390, "height": 844}
  },
  "events": [
    {"type": "search", "query": "무선 이어폰", "ts": "..."},
    {"type": "view", "product_id": "pd_1", "ts": "..."},
    {"type": "add_cart", "product_id": "pd_1", "ts": "..."}
  ],
  "context": {
    "utm": {"source": "google", "medium": "cpc"},
    "location": {"region": "서울", "city": "강남구"}
  },
  "summary": {
    "total_events": 5,
    "converted": true
  }
}
```

## 성능 실험

### 실험 1: 정렬 저장 효과

데이터를 어떻게 정렬하여 저장하느냐에 따른 쿼리 성능 차이를 측정합니다.

```bash
# Airflow에서 성능 테스트 DAG 실행
# 또는 직접 실행
./scripts/submit-streaming.sh
docker exec spark-master spark-submit /opt/spark-jobs/batch/performance_test.py 10000000
```

**예상 결과:**

| 쿼리 | 비정렬 | timestamp 정렬 | product+time 정렬 | 개선율 |
|------|--------|----------------|-------------------|--------|
| 시간대 조회 | 5.2s | 1.8s | 2.1s | 65% |
| 상품 조회 | 4.8s | 4.5s | 0.9s | 81% |
| 복합 조회 | 6.1s | 2.2s | 0.7s | 89% |

### 실험 2: 분산 처리 스케일링

워커 수 증가에 따른 처리량 변화를 측정합니다.

| 워커 수 | 처리 시간 | 처리량 (records/sec) | 스케일링 효율 |
|---------|-----------|---------------------|---------------|
| 1 | 120s | 83,333 | - |
| 2 | 65s | 153,846 | 92% |
| 4 | 35s | 285,714 | 86% |

## 트래픽 패턴

시간대별 트래픽 시뮬레이션:

```
        이벤트 수
          │            ┌───┐
          │  ┌─┐ ┌─┐   │   │
          │  │ │ │ │   │   │
          │──┴─┴─┴─┴───┴───┴──▶ 시간
            8시  12시   20시
          (출근)(점심) (저녁 피크)
```

- **새벽 (0-6시)**: 최소 트래픽 (10%)
- **점심 (12-14시)**: 첫 번째 피크 (80%)
- **저녁 (20-22시)**: 최대 피크 (100%)
- **세일 기간**: 평소 대비 10배

## 면접 대비 Q&A

### Q: 왜 Hadoop 대신 MinIO를 선택했나요?

A: HDFS의 NameNode가 단일 장애점(SPOF)이고, 작은 파일이 많을 때 메모리 문제가 발생합니다. MinIO는 이런 문제가 없고 S3 호환이라 클라우드 마이그레이션도 용이합니다.

### Q: 왜 Hive 대신 Iceberg를 선택했나요?

A: Hive는 파일 경로 수준의 메타데이터만 가지고 있어서 쿼리 시 모든 파일을 스캔해야 합니다. Iceberg는 파일 내부 데이터 레벨의 메타데이터(min/max 값 등)를 가지고 있어서 불필요한 파일을 스킵할 수 있습니다. 실험 결과 최대 89%의 쿼리 성능 향상을 확인했습니다.

### Q: 정렬 저장의 효과는 어떻게 측정했나요?

A: 동일한 1000만 건의 데이터를 세 가지 방식으로 저장하고 동일한 쿼리의 실행 시간을 비교했습니다:
1. 랜덤 순서 (비정렬)
2. timestamp 기준 정렬
3. product_id + timestamp 기준 정렬

### Q: 비정형 데이터는 어떻게 처리했나요?

A: 세 가지 유형의 비정형 데이터를 처리했습니다:
1. **리뷰 텍스트**: 키워드 추출, 감성 분석 (긍정/부정 단어 빈도)
2. **검색 쿼리**: 패턴 분류, 오타 포함 자연어 처리
3. **Nested JSON**: Spark의 StructType으로 스키마 정의 후 flatten 처리

## 라이센스

MIT License

## 기여

PR과 이슈는 언제나 환영합니다!
