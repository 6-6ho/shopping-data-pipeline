# 이커머스 실시간 데이터 파이프라인 프로젝트

> 비정형 데이터(리뷰 텍스트, Nested JSON) + 실시간/배치 처리 + 클러스터 스케일링

---

## 프로젝트 개요

**주제:** 이커머스 쇼핑 이벤트 실시간 분석 플랫폼

**스토리:**
- 쇼핑몰에서 검색/클릭/구매/리뷰 이벤트 발생
- 시간대별 트래픽 변화 (저녁 피크, 세일 기간 폭증)
- 실시간 매출 대시보드 + 일별 배치 분석
- 트래픽에 따른 클러스터 스케일링

**차별화 포인트:**
- 비정형 데이터 처리 (리뷰 텍스트, Nested JSON)
- 정렬 저장 vs 비정렬 저장 성능 비교 (수치화)
- Standalone → 분산 클러스터 성능 개선 (수치화)

---

## 기술 스택

| 기술 | 역할 | 선택 이유 |
|------|------|-----------|
| **Kafka** | 실시간 스트리밍 | 데이터 유실 방지, 버퍼링 |
| **Spark** | 데이터 처리 | Streaming/Batch 통합, 분산 처리 |
| **MinIO** | 오브젝트 스토리지 | HDFS NameNode SPOF 문제 회피, S3 호환 |
| **Iceberg** | 테이블 포맷 | Hive 대비 data skipping으로 쿼리 성능 향상 |
| **Airflow** | 배치 스케줄링 | DAG로 의존성 관리, 모니터링 |
| **Grafana** | 대시보드 | 실시간 모니터링, 성능 비교 시각화 |
| **Docker** | 컨테이너화 | 재현 가능한 환경, 손쉬운 스케일링 |

---

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
│                             │
│  - raw layer (원본)         │
│  - processed layer (정제)   │
│  - aggregated layer (집계)  │
└─────────────┬───────────────┘
              │
              ▼
┌─────────────────────────────┐
│          Grafana            │
│  - 실시간 매출 대시보드     │
│  - 클러스터 모니터링        │
│  - 성능 비교 차트           │
└─────────────────────────────┘
```

---

## Phase 1: 인프라 구축

### Task 1.1: 프로젝트 구조 생성
```
shopping-data-pipeline/
├── docker/
│   ├── docker-compose.yml
│   ├── spark/
│   │   └── Dockerfile
│   └── airflow/
│       └── Dockerfile
├── data-generator/
│   ├── main.py
│   ├── generators/
│   ├── data/
│   └── requirements.txt
├── spark-jobs/
│   ├── streaming/
│   └── batch/
├── airflow/
│   └── dags/
├── grafana/
│   └── dashboards/
├── scripts/
│   └── init-minio.sh
└── README.md
```

### Task 1.2: Docker Compose 작성
```yaml
# 필요한 서비스:
- zookeeper
- kafka
- minio
- spark-master
- spark-worker (1개로 시작, 나중에 스케일)
- airflow-webserver
- airflow-scheduler
- airflow-worker
- postgres (Airflow 메타데이터)
- grafana
```

### Task 1.3: MinIO 설정
- [ ] 버킷 생성: `raw`, `processed`, `aggregated`
- [ ] 접근 키 설정
- [ ] Iceberg 카탈로그 연동

### Task 1.4: Kafka 토픽 생성
- [ ] `shopping-events` (클릭, 장바구니, 구매)
- [ ] `reviews` (리뷰 텍스트)
- [ ] `search-queries` (검색 쿼리)

### Task 1.5: Spark + Iceberg 연동
- [ ] Spark 이미지에 Iceberg JAR 추가
- [ ] MinIO를 S3 호환 스토리지로 설정
- [ ] Iceberg 카탈로그 설정

### Task 1.6: 기본 연결 테스트
- [ ] Kafka 메시지 produce/consume 테스트
- [ ] Spark에서 MinIO 읽기/쓰기 테스트
- [ ] Iceberg 테이블 생성 테스트

---

## Phase 2: 데이터 생성기 개발

### Task 2.1: 기본 구조 작성
```python
# data-generator/main.py
- Kafka producer 설정
- 시간대별 트래픽 패턴 적용
- 메인 루프
```

### Task 2.2: 쇼핑 이벤트 생성기
```python
# generators/shopping_event.py

이벤트 타입:
- search: 상품 검색
- view: 상품 상세 조회
- add_cart: 장바구니 추가
- purchase: 구매 완료

필드:
- event_id
- event_type
- user_id
- product_id
- category
- price
- timestamp
- session_id
- device (nested)
- context (nested)
```

### Task 2.3: 리뷰 텍스트 생성기 (비정형)
```python
# generators/review.py

방법 1: 템플릿 조합
TEMPLATES = {
    "delivery": ["배송 빨라요", "배송이 늦었어요", "배송은 보통"],
    "quality": ["품질 좋아요", "가격대비 괜찮아요", "별로에요"],
    "packaging": ["포장 꼼꼼해요", "포장이 아쉬워요"],
    "overall": ["재구매 의사 있어요", "다신 안살듯", "보통이에요"]
}

방법 2: 실제 데이터셋 활용
- AI Hub 상품 리뷰 데이터셋
- Kaggle 한국어 리뷰 데이터셋

필드:
- review_id
- product_id
- user_id
- rating (1-5)
- text (비정형 텍스트)
- images (배열)
- created_at
```

### Task 2.4: 검색 쿼리 생성기 (비정형)
```python
# generators/search_query.py

SEARCH_PATTERNS = [
    "{brand} {item} {size}",      # "나이키 운동화 270"
    "{item} {color} 추천",         # "원피스 검정 추천"
    "가성비 {item}",               # "가성비 청바지"
    "{item} 추천 {year}",          # "노트북 추천 2024"
]
```

### Task 2.5: Nested JSON 세션 이벤트 생성기 (비정형)
```python
# generators/session_event.py

{
  "session_id": "sess_xxx",
  "user_id": "u_xxx",
  "device": {
    "type": "mobile",
    "os": "iOS",
    "app_version": "5.2.1"
  },
  "events": [
    {"type": "search", "query": "무선 이어폰", "ts": "..."},
    {"type": "view", "product_id": "pd_1", "ts": "..."},
    {"type": "add_cart", "product_id": "pd_1", "ts": "..."}
  ],
  "context": {
    "referrer": "google_search",
    "campaign": {"id": "camp_123", "name": "여름세일"}
  }
}
```

### Task 2.6: 시간대별 트래픽 패턴
```python
# generators/traffic_pattern.py

HOURLY_PATTERN = {
    0: 0.1,   # 새벽 - 최소
    6: 0.2,   # 아침
    10: 0.4,  # 오전
    12: 0.8,  # 점심 피크
    14: 0.5,  # 오후
    18: 0.7,  # 퇴근
    20: 1.0,  # 저녁 피크
    22: 0.8,  # 밤
    23: 0.4,  # 늦은 밤
}

# 세일 기간 시뮬레이션
SALE_MULTIPLIER = 10  # 평소 대비 10배
```

### Task 2.7: 시드 데이터 준비
```
data/
├── products.csv        # 상품 목록 (id, name, category, price)
├── categories.csv      # 카테고리 목록
├── brands.csv          # 브랜드 목록
├── review_templates/   # 리뷰 템플릿
└── search_keywords/    # 검색 키워드 목록
```

---

## Phase 3: 실시간 파이프라인 (Spark Streaming)

### Task 3.1: Iceberg 테이블 스키마 설계
```sql
-- 쇼핑 이벤트 테이블
CREATE TABLE shopping_events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    product_id STRING,
    category STRING,
    price DECIMAL(10,2),
    timestamp TIMESTAMP,
    session_id STRING,
    device STRUCT<type: STRING, os: STRING, app_version: STRING>,
    context STRUCT<referrer: STRING, campaign: STRUCT<id: STRING, name: STRING>>
)
PARTITIONED BY (date(timestamp), event_type)
-- 정렬 키: timestamp, product_id

-- 리뷰 테이블
CREATE TABLE reviews (
    review_id STRING,
    product_id STRING,
    user_id STRING,
    rating INT,
    text STRING,
    images ARRAY<STRING>,
    created_at TIMESTAMP
)
PARTITIONED BY (date(created_at))
-- 정렬 키: product_id, created_at
```

### Task 3.2: Kafka → Spark Streaming 연동
```python
# spark-jobs/streaming/shopping_stream.py

- Kafka에서 이벤트 읽기
- JSON 파싱 (nested 구조 포함)
- Iceberg 테이블에 쓰기
- 체크포인팅 설정
```

### Task 3.3: 실시간 집계
```python
# spark-jobs/streaming/realtime_aggregation.py

5분 윈도우 집계:
- 카테고리별 매출
- 인기 상품 TOP 10
- 이벤트 타입별 카운트
```

### Task 3.4: Grafana 실시간 대시보드 연결
- [ ] 데이터 소스 연결
- [ ] 실시간 매출 차트
- [ ] 인기 검색어 차트
- [ ] 이벤트 처리량 (records/sec)

---

## Phase 4: 배치 파이프라인 (Airflow)

### Task 4.1: Airflow DAG 구조
```python
# airflow/dags/daily_analytics.py

daily_analytics_dag:
├── check_data_quality    # 데이터 품질 체크
├── aggregate_daily_sales # 일별 매출 집계
├── analyze_reviews       # 리뷰 분석 (키워드 추출)
├── calculate_metrics     # 전환율, 재구매율 계산
└── export_report         # 리포트 생성
```

### Task 4.2: 일별 집계 Job
```python
# spark-jobs/batch/daily_aggregation.py

집계 항목:
- 일별 총 매출
- 카테고리별 매출
- 시간대별 트래픽 분포
- 상품별 조회/구매 전환율
```

### Task 4.3: 리뷰 분석 Job
```python
# spark-jobs/batch/review_analysis.py

분석 항목:
- 리뷰 키워드 추출 (배송, 품질, 가격 등)
- 긍정/부정 단어 빈도
- 별점 분포
- 상품별 평균 별점
```

### Task 4.4: 데이터 품질 체크
```python
# spark-jobs/batch/data_quality.py

체크 항목:
- NULL 값 비율
- 중복 이벤트 체크
- 타임스탬프 유효성
- 필수 필드 존재 여부
```

---

## Phase 5: 성능 실험 (핵심!)

### 실험 1: 정렬 저장 효과

#### 실험 설계
```
데이터: 동일한 데이터 1000만 건

비교군:
A) 랜덤 순서로 저장
B) timestamp 기준 정렬 후 저장
C) product_id + timestamp 기준 정렬 후 저장

테스트 쿼리:
1. 특정 시간대 조회: WHERE timestamp BETWEEN ... AND ...
2. 특정 상품 조회: WHERE product_id = 'xxx'
3. 범위 + 상품: WHERE product_id = 'xxx' AND timestamp BETWEEN ...
```

#### 측정 항목
```
- 쿼리 응답 시간 (ms)
- 스캔한 파일 수
- 읽은 데이터 양 (bytes)
- Data skipping 효과
```

#### 결과 기록
| 쿼리 | 비정렬 | timestamp 정렬 | product+time 정렬 | 개선율 |
|------|--------|----------------|-------------------|--------|
| Q1   |        |                |                   |        |
| Q2   |        |                |                   |        |
| Q3   |        |                |                   |        |

### 실험 2: 분산 처리 효과

#### 실험 설계
```
데이터: 1억 건 이벤트

비교군:
A) Standalone (1 worker)
B) 2 workers
C) 4 workers

테스트 작업:
1. 전체 데이터 집계 (GROUP BY category)
2. 조인 쿼리 (events JOIN products)
3. 윈도우 함수 (시간대별 이동 평균)
```

#### 측정 항목
```
- 작업 완료 시간
- 처리량 (records/sec)
- 리소스 사용률
```

#### 결과 기록
| 작업 | 1 worker | 2 workers | 4 workers | 스케일링 효율 |
|------|----------|-----------|-----------|---------------|
| 집계 |          |           |           |               |
| 조인 |          |           |           |               |
| 윈도우|         |           |           |               |

### 실험 3: 클러스터 스케일링 시연

#### 시나리오
```
시간대별 스케일링:
- 새벽 (트래픽 10%): worker 1개
- 점심 (트래픽 80%): worker 2개
- 저녁 (트래픽 100%): worker 4개
- 세일 (트래픽 1000%): worker 8개
```

#### Grafana 대시보드
```
- 패널 1: 시간대별 트래픽
- 패널 2: 활성 worker 수
- 패널 3: 처리 지연 시간
- 패널 4: 리소스 사용률
```

---

## Phase 6: 문서화 및 완성

### Task 6.1: README 작성
```markdown
# 이커머스 실시간 데이터 파이프라인

## 개요
## 기술 스택 (선택 이유 포함)
## 빠른 시작
## 성능 실험 결과
## 데모 시나리오
```

### Task 6.2: 아키텍처 다이어그램
- [ ] draw.io / Excalidraw로 작성
- [ ] 데이터 흐름 표시
- [ ] 기술 로고 포함

### Task 6.3: 데모 스크립트
```bash
# demo.sh
docker-compose up -d
# 데이터 생성 시작
# Grafana 접속 안내
# 세일 모드 트리거
```

### Task 6.4: 면접 대비
```
Q: 왜 Hadoop 대신 MinIO?
A: NameNode SPOF 문제, Small Files Problem 회피

Q: 왜 Hive 대신 Iceberg?
A: Data skipping으로 쿼리 성능 향상

Q: 정렬 저장의 효과는?
A: [실험 결과 수치]

Q: 분산 처리 효과는?
A: [실험 결과 수치]
```

---

## 체크리스트

### Phase 1: 인프라 구축
- [ ] 1.1 프로젝트 구조 생성
- [ ] 1.2 Docker Compose 작성
- [ ] 1.3 MinIO 설정
- [ ] 1.4 Kafka 토픽 생성
- [ ] 1.5 Spark + Iceberg 연동
- [ ] 1.6 기본 연결 테스트

### Phase 2: 데이터 생성기
- [ ] 2.1 기본 구조 작성
- [ ] 2.2 쇼핑 이벤트 생성기
- [ ] 2.3 리뷰 텍스트 생성기 (비정형)
- [ ] 2.4 검색 쿼리 생성기 (비정형)
- [ ] 2.5 Nested JSON 세션 이벤트 생성기
- [ ] 2.6 시간대별 트래픽 패턴
- [ ] 2.7 시드 데이터 준비

### Phase 3: 실시간 파이프라인
- [ ] 3.1 Iceberg 테이블 스키마 설계
- [ ] 3.2 Kafka → Spark Streaming 연동
- [ ] 3.3 실시간 집계
- [ ] 3.4 Grafana 대시보드 연결

### Phase 4: 배치 파이프라인
- [ ] 4.1 Airflow DAG 구조
- [ ] 4.2 일별 집계 Job
- [ ] 4.3 리뷰 분석 Job
- [ ] 4.4 데이터 품질 체크

### Phase 5: 성능 실험
- [ ] 5.1 정렬 저장 효과 실험
- [ ] 5.2 분산 처리 효과 실험
- [ ] 5.3 클러스터 스케일링 시연

### Phase 6: 문서화
- [ ] 6.1 README 작성
- [ ] 6.2 아키텍처 다이어그램
- [ ] 6.3 데모 스크립트
- [ ] 6.4 면접 대비 자료

---

## 참고 자료

### 공식 문서
- [Apache Spark](https://spark.apache.org/docs/latest/)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [MinIO](https://min.io/docs/minio/linux/index.html)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [Grafana](https://grafana.com/docs/)

### 데이터셋
- [AI Hub 상품 리뷰](https://aihub.or.kr/)
- [Kaggle Korean Reviews](https://www.kaggle.com/)
