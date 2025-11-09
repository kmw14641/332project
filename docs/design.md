## Assumption
- disk는 input data의 2~3배가 존재합니다. (shuffle phase + alpha)

## Memory Sort Phase

- N개의 Worker machine이 각각 시작 신호를 받습니다. (어떻게 받게 되는지는 TBD)
- 각각이 가진 M개의 input file들을 sort library를 사용해 RAM에서 정렬합니다.  
  input file을 순회하면서, 읽기 -> 정렬 -> 쓰기 과정을 반복합니다.
- 아직 병렬 연산은 고려하지 않습니다.
- 각 파일 내부는 정렬된 상태이며, 각 파일의 값의 범위는 어떤 범위든 가질 수 있습니다.

## Disk Merge Phase

- 두 파일을 읽은 후, 각각 iterator를 생성하고, 더 작은 쪽의 iterator를 증가시키며 output을 만드는 merge phase를 수행합니다.
- iterator의 범위는 파일 하나에서 시작하여 merge 단계가 진행될 때마다 두 배가 되며, log_2(n)의 단계를 거쳐 완료됩니다.
- disk I/O를 줄이기 위해 파일 읽기/쓰기는 파일 전체를 단위로 하며, merge 단계에서 file size * 3만큼의 메모리가 필요합니다.
- 수행 결과 1st file ~ Mth file까지의 데이터가 정렬됩니다.

## Sampling Phase

- N개의 Worker machine에서 작은 용량의 sample을 취해 데이터의 분포를 조사합니다.
- Memory Sort phase가 시작됨과 동시에 worker가 sample 채취 후 master에 전송을 시작합니다.
- master가 모든 worker의 sample을 모아 in-memory 정렬을 수행한 후, 균등한 개수로 잘라 key range를 조사합니다.
- 각 Worker는 자신이 할당될 key range와 다른 worker들의 key range를 저장합니다.
- 이 phase는 slicing phase 이전에 완료됨이 보장되어야 합니다.

## Labeling Phase

- disk merge가 끝나면, worker는 자신이 가진 파일들을 '다른 worker에게 전송 가능한 형태'로 구성합니다. 
  1) 파일 자르기: 파일 중간에 전송할 worker의 key range를 넘어가면 파일을 slice합니다. input file의 끝에 도달하면 따로 합치지 않고 그대로 끝냅니다.
  2) 파일 라벨링: 자르기가 끝난 파일은 전송할 worker를 지정합니다.

## Synchronization Phase
- shuffle phase를 시작하기 위한 동기화를 진행합니다.
  1) 파일 정보 전송: sender가 receiver에게 어떤 파일들을 보내야 하는지에 대한 정보를 전송합니다.
  2) 완료 표시: shuffle 전 worker의 모든 작업이 끝났음을 master에게 알립니다.
  3) shuffle phase 시작 명령: master가 모든 worker의 완료 표시를 기다린 후 일괄적으로 shuffle phase 시작 명령을 내립니다.

## Shuffle Phase
- 받아야 하는 파일 정보를 토대로, 각 worker가 다른 worker에게 직접 파일 전송을 요청하고, 응답으로 파일을 받습니다.
  - 주체가 받는 worker인 이유 : disk merge phase 시작에 유리하고, fault tolerance 구현을 무거운 파일 전송 대신 가벼운 파일 정보 전송 단계로 옮길 수 있으며, 요청 조절 시 머신의 정보를 활용할 수 있습니다.
- 통신 방식: gRPC를 사용할 것이며, 요청을 받는 서버 스레드 풀, 요청을 전송하는 요청 스레드 풀이 있습니다. 
  서버 스레드 풀은 기본 스레드 풀 (코어 개수 사용), 요청 스레드 풀은 전용 fixed thread 풀을 사용하며, 병목으로 인한 메모리 초과를 방지하기 위해 요청 시 blocking IO를 사용합니다.
- 파일 삭제는 전체 phase가 끝난 후 진행합니다. 빠른 삭제가 필요하다면 모두가 final merge phase에 들어갔음을 신호로 받을 수 있습니다. shuffle phase 중간에 삭제는 고려하지 않았습니다.

## Final Merge Phase
- 파일을 모두 받으면 disk merge phase를 시작합니다. 다른 머신과 동기화하지 않아도 됩니다.
- 전에 있던 Disk Merge Phase와 비슷하게 진행합니다.
- master에게 완료 신호를 보냅니다.
- master는 모두 완료되는 것을 기다린 후 완료 메시지를 출력합니다.

## Fault tolerance

### Memory Sort Phase
TBD

### Disk Merge Phase
TBD

### Sampling Phase
TBD

## Synchronization Phase
- 파일 정보 전송: 파일 정보를 보내던 도중 워커가 죽을 수 있습니다. 전송이 완료되었음을 응답으로 받아 디스크에 기록하고, 워커가 켜졌을 때 완료되지 않은 전송을 재시작합니다.
  - 전송에 성공은 했지만 전송 성공 응답을 디스크에 기록하기 전에 워커가 죽을 수 있습니다. 같은 정보를 두 번 보내도 문제가 없도록 worker를 key로 한 dictionary를 활용해 overwrite합니다.
  - overwrite가 괜찮은 이유: 전송이 완료된 후 워커가 죽었는지, 그 전에 죽었는지에 관계 없이 받아야 할 정보가 동일합니다. (아닌 예: 정보를 받을 때 +=, concat 등의 연산을 하는 경우)
- 완료 표시: 마찬가지로 전송 성공 응답을 받아 기록하도록 하고, 완료 여부는 overwrite합니다.
- 시작 명령: master가 명령을 보내고 worker에게 도달하기 전에 worker가 죽을 수 있습니다. 전송 성공 응답을 모두 받을 때까지 while 루프를 돕니다.

### Shuffle Phase
- 파일 진행 상황을 수신 worker에 기록해놓고, 이전 진행 상황에 이어서 다시 요청을 진행하면 fault tolerance를 구현할 수 있습니다.

## State Restoration
- 재시작 시 이전 워커의 진행 상태, 복구 동작을 정의하고 구현합니다.

## Further optimization
- Sort Phase, Merge Phase 에서 병렬 연산: single disk I/O의 bottleneck을 직접 확인해본다.
- key-value optimization: value가 90바이트인 점때문에 key-value를 직접 copy하는 것보다 포인터 참조 연산을 하면서 sort하는 것이 빠르다. key+(value index)쌍을 활용해 sort함으로써 참조 연산도 줄이고 copy overhead도 줄이는 방법이 더 빠른지 확인해본다.
- file copy optimization: os heap -> jvm heap copy 연산을 줄이는 최적화