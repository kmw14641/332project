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
- Milestone 2-3: 머신 내 정렬 - ⭐completed⭐
- Milestone 2-4: synchronization phase 완료 (fault tolerance 고려) - ⭐completed⭐
- Milestone 2-5: shuffle phase 완료 (fault tolerance 고려) - ⭐completed⭐
- Milestone 2-6: state restoration 구현 - ⭐completed⭐
- Milestone 2-7: final merge phase 완료 - ⭐completed⭐

## Milestone 3 : 디버깅
- ⭐completed⭐
## Milestone 4 : 테스트
- Unit test - 필요한 로직이 많지 않음
- Integrated test - 우선순위가 높지 않고, 시간 부족으로 인해 e2e 테스트로 대체

## Milestone 5 : Optimization
- ⭐completed⭐