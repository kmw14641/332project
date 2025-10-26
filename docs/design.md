## Assumption
- input file 3개를 메모리에 올릴 수 있습니다. 이 가정이 틀리다면, 즉 특정 worker의 RAM size가 input file size * 3보다 작다면, 구현의 간결성을 위해 가정을 만족할 때까지 2등분하는 phase를 추가합니다.

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
  3) 파일 정보 전송: master에게 어떤 워커에게 어떤 파일들을 보내야 하는지에 대한 정보를 전송합니다.

## Shuffle Phase

- 모든 worker의 labeling phase가 끝나, 모든 정보가 master에 취합되었을 때 shuffling phase를 시작합니다.
- master는 각 worker가 다른 worker로부터 몇개의 데이터를 받아야 하는지 정보를 전송합니다.
- 받아야 하는 파일 정보를 토대로, 각 worker가 다른 worker에게 직접 파일 전송을 요청하고, 응답으로 파일을 받습니다.
  - 주체가 받는 worker인 이유 : 가용 RAM 정보를 알 수 있어 과도한 요청이 오는 케이스를 처리하지 않을 수 있고, fault tolerance 구현 시에도 전송 상태가 최종적으로 저장되는 곳과 요청 주체가 동일하기 때문에 구현에 유리합니다.
- 통신 방식: gRPC를 사용할 것이며, 요청을 받는 서버 스레드, 요청을 전송하는 전송 스레드가 있고, 각 요청마다 스레드를 사용할 것인지 async runtime을 사용할 것인지 등은 gRPC 프레임워크와 가장 자연스럽게 융합되는 형태로 선택한다.

## Final Merge Phase
- Disk Merge Phase와 비슷하게 진행합니다.

## Fault tolerance

### Memory Sort Phase
TBD

### Disk Merge Phase
TBD

### Sampling Phase
TBD

### Shuffle Phase
- 파일 진행 상황을 수신 worker에 기록해놓고, 이전 진행 상황에 이어서 다시 요청을 진행하면 fault tolerance를 구현할 수 있습니다.

## Further optimization
- Sort Phase, Merge Phase 에서 병렬 연산: single disk I/O의 bottleneck을 직접 확인해본다.
- key-value optimization: value가 90바이트인 점때문에 key-value를 직접 copy하는 것보다 포인터 참조 연산을 하면서 sort하는 것이 빠르다. key+(value index)쌍을 활용해 sort함으로써 참조 연산도 줄이고 copy overhead도 줄이는 방법이 더 빠른지 확인해본다.