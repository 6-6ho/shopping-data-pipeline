# 🛍️ Handover: Applying Trade Helper Deployment Patterns to Shop Project

**"Trade Helper에서 성공한 배포 경험(Docker Compose + Nginx Reverse Proxy)을 그대로 샵 프로젝트에 이식하자."**

## 🎯 Request Prompt (For Next Session)
다음 세션의 AI에게 아래 내용을 전달하세요. 이 프롬프트는 우리가 **어떻게 문제를 해결했는지**에 대한 맥락을 포함하고 있습니다.

---
```markdown
방금 **Trade Helper(Crypto Portfolio)** 프로젝트를 성공적으로 배포했어.
여기서 얻은 **"Docker Compose + Nginx Reverse Proxy"** 배포 패턴을 **Shop(User Behavior Analytics)** 프로젝트에도 똑같이 적용해서 배포하려고 해.

**[Trade Helper에서 확립한 배포 성공 방정식]**
1. **인프라 구조**: `junho.in` (랜딩)과 `trade.junho.in` (앱)을 Nginx 하나로 라우팅해서 처리함.
2. **컨테이너 네트워크**: 모든 서비스(Frontend, API, DB)를 하나의 Docker Network(`appnet`)로 묶어서 통신시킴.
3. **데이터 흐름**: Spark/Kafka가 처리한 데이터를 PostgreSQL에 적재하고, Web에서 그걸 조회하는 구조가 안정적이었음.

**[이번 목표: Shop 프로젝트 배포 (Docker Network 전략)]**
"Host IP 하드 코딩" 대신, **Docker Network를 공유**하는 정석적인 방법으로 배포하려고 해.

1. **Deployment**: `docker-compose.yml`에서 Streamlit 서비스 이름을 `shop-frontend`로 지정해줘 (Nginx가 이 이름으로 찾음).
2. **Network**: Trade Helper가 만들어둔 네트워크(`trade-helper_appnet`)에 **External Network**로 붙어야 해.
3. **Data Pipeline**: Spark가 PostgreSQL에 데이터를 저장하고, Streamlit이 읽는 구조 유지.

지금 내 Shop 프로젝트 상태를 보고, 위 요건(External Network, Service Name)에 맞춰 `docker-compose.yml`을 작성해줘.
```
---

## 💡 Key Takeaways to Apply (성공 요인 이식)
1. **Unified Network**: `docker-compose.yml` 맨 아래에 아래 설정을 꼭 추가해야 함.
   ```yaml
   networks:
     appnet:
       external: true
       name: trade-helper_appnet  # Trade Helper가 만든 실제 네트워크 이름
   ```
2. **Service Name**: Streamlit 컨테이너에 `container_name: shop-frontend`를 명시해야 Nginx DNS가 찾을 수 있음.
3. **Architecture**: K8s까지는 과하고, Docker Network 공유가 가장 깔끔한 베스트 프랙티스임.
