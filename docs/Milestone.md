# Milestones

## Milestone 1: 개발환경 설정
- java17, scala 2.13, sbt 1.11.7
- Milestone 1-1 : 서버 환경 세팅 - ⭐completed⭐
  - 실행환경 업데이트
  - 코드 또는 binary 설치
- Milestone 1-2 : 로컬 환경 세팅 - ⭐completed⭐
  - Docker compose
  - 각 worker에 64MB RAM 할당 (디스크는 고려하지 않는다)
  - Docker network로 container끼리 통신 가능한지 확인

## Milestone 2 : 구현 완료
- Milestone 2-1: 프로그램 시작 구현 - ⭐completed⭐
- Milestone 2-2: 샘플링 - ⭐completed⭐
- Milestone 2-3: 머신 내 정렬
- Milestone 2-4: synchronization phase 완료 (fault tolerance 고려)
- Milestone 2-5: shuffle phase 완료 (fault tolerance 고려)
- Milestone 2-6: state restoration 구현
- Milestone 2-7: final merge phase 완료

## Milestone 3 : 디버깅

## Milestone 4 : 테스트
- Unit test
- Integrated test

## Milestone 5 : Optimization
- 병렬 연산
- key-value optimization
- duckDB
  - 통계 분석용 DB, 빠른 input & sort 지원
  - file을 주고 빠르게 disk based sort를 하기 위함
  - fault tolerance 자동 제어 : 직접 구현했을 때와 성능 비교

## Milestone 6 : Web development (optional)

## Milestone 7 : 발표준비