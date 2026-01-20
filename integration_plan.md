# 🏗️ Service Integration Plan: From "Project" to "Platform"

**Vision**: `Trade Helper`의 인프라를 **"Junho Data Platform"**으로 승격시킵니다.
이제 새로운 프로젝트(Shop, Coin ML 등)는 무거운 인프라를 띄우지 않고, 이 플랫폼에 **"플러그인"**처럼 로직만 추가합니다.

## 📊 Services Comparison (중복 현황)

| Service Role | Trade Helper (현재) | Shop (통합 예정) | Future Platform Name |
|---|---|---|---|
| **Message Queue** | Kafka (Single) | Kafka (Single) | **Shared Kafka** (`kafka:9092`) |
| **Database** | Postgres (v16) | Postgres (v15) | **Shared Postgres** (`postgres:5432`) |
| **Object Storage**| MinIO | MinIO | **Shared MinIO** (`minio:9000`) |
| **Compute** | Spark (Simple) | Spark Cluster | **Shared Spark Cluster** (`spark-master:7077`) |
| **Orchestrator** | Airflow | Airflow | **Shared Airflow** (`airflow-webserver:8080`) |
| **Network** | `appnet` | `pipeline-network` | **`data-platform-net`** (Renamed) |

## 🚀 Execution Steps (실행 계획)

### 1단계: Platform Generalization (플랫폼화)
기존 `Trade Helper`의 `docker-compose.yml`을 수정하여 **범용적인 이름**으로 변경합니다.
(서비스 이름이 이미 Kafka, Postgres 등으로 되어 있어 크게 손댈 건 없지만, `appnet` 같은 네트워크 명칭을 명확히 합니다.)

### 2단계: Workload Migration (작업 이관)
새로운 서비스를 띄우는 게 아니라, **기존 플랫폼에 리소스만 추가**하는 방식으로 바뀝니다.

*   **Spark**: "새 클러스터를 만든다" (X) → "Shared Spark에 **Job 파일(.py)**만 던진다" (O)
*   **Airflow**: "새 스케줄러를 띄운다" (X) → "Shared Airflow에 **DAG 파일(.py)**만 넣는다" (O)
*   **Kafka**: "새 브로커를 깐다" (X) → "Shared Kafka에 **Topic**만 생성한다" (O)
*   **Database**: "새 DB를 깐다" (X) → "Shared Postgres에 **Database/User**만 추가한다" (O)

### 3단계: Shop "App" Deployment (앱 배포)
Shop 프로젝트는 이제 **아주 얇은(Thin) 클라이언트**가 됩니다.

**[Shop Docker Compose (Final)]**
```yaml
services:
  # 인프라 없음! 오직 로직만 존재
  
  shop-generator:
    image: python:3.9
    command: python generate_data.py --dest kafka:9092  # 플랫폼 카프카로 쏨
    networks: [data-platform-net]

  shop-dashboard:
    image: streamlit
    command: streamlit run app.py --db postgres:5432    # 플랫폼 DB에서 읽음
    networks: [data-platform-net]

networks:
  data-platform-net:
    external: true
    name: trade-helper_appnet  # (또는 data-platform-net으로 개명)
```

## 🏆 Final Benefit
이제 **"프로젝트 시작 = 인프라 구축"**이라는 공식이 깨집니다.
**"프로젝트 시작 = 비즈니스 로직(Code) 작성"**으로 생산성이 극대화됩니다.
