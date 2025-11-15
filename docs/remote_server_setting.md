# Remote Server Setup Guide

## 사전 준비

### PSSH (Parallel SSH) 설치
Master에 PSSH를 설치하고 SSH 핑거프린트를 등록합니다:

```bash
pip install --user pssh
# SSH 핑거프린트를 20개 worker에 수작업으로 등록
```

### Host 파일 준비
Host 리스트를 텍스트 파일로 준비합니다 (예: `~/hosts/hosts1-10.txt`):

```bash
# 기본 PSSH 사용법
pssh -h hosts1.txt -i "ls"
```

---

## 배포 프로세스

### 1. Binary 배포
Master에서 git clone 후 컴파일한 뒤, PSCP를 사용하여 배포:

```bash
pscp -h ~/hosts/hosts1-10.txt worker/target/scala-2.13/worker.jar /home/cyan
```

### 2. Worker 실행
배포된 JAR 파일 실행:

```bash
pssh -h ~/hosts/hosts1-10.txt -i "bash -l -c 'java -XX:MaxDirectMemorySize=2g -jar worker.jar > worker.log 2>&1'"
```

---

## 로그 관리

### 로그 확인
SSH 접속 후 실시간 로그 확인:

```bash
ssh [worker]
tail -f worker.log
```

---

## 포트 관리

### 고정포트 강제 종료
특정 포트(예: 8080)를 사용 중인 프로세스 종료:

```bash
pssh -h ~/hosts/hosts1-10.txt -i "lsof -t -i :8080 | xargs -r kill -9"
```

---

## 환경 설정

### Java 설치 (OpenJDK 17)

```bash
# 1. 다운로드
pssh -h ~/hosts/hosts2-10.txt -t 0 -i "curl -o tmp/openjdk17.tar.gz https://download.java.net/java/GA/jdk17/0d483333a00540d886896bac774ff48b/35/GPL/openjdk-17_linux-x64_bin.tar.gz"

# 2. 디렉토리 생성
pssh -h hosts2-10.txt -i "mkdir ~/.local/java"

# 3. 압축 해제
pssh -h hosts2-10.txt -i "tar -xvf tmp/openjdk17.tar.gz -C ~/.local/java"

# 4. 환경 변수 설정 (bashrc)
pssh -h hosts2-10.txt -i "echo 'export JAVA_HOME=\$HOME/.local/java/jdk-17' >> ~/.bashrc"
pssh -h hosts2-10.txt -i "echo 'export PATH=\$JAVA_HOME/bin:\$PATH' >> ~/.bashrc"

# 5. 환경 변수 설정 (bash_profile)
pssh -h hosts2-10.txt -i "echo 'export JAVA_HOME=\$HOME/.local/java/jdk-17' >> ~/.bash_profile"
pssh -h hosts2-10.txt -i "echo 'export PATH=\$JAVA_HOME/bin:\$PATH' >> ~/.bash_profile"

# 6. 설치 확인
pssh -h hosts2-10.txt -i "bash -l -c 'java -version'"
```

### GenSort 설치

```bash
# 1. 디렉토리 생성
pssh -h hosts2-10.txt -i "mkdir tmp"

# 2. 다운로드
pssh -h hosts2-10.txt -i "curl -o tmp/gensort.tar.gz https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz"

# 3. 압축 해제 준비
pssh -h hosts2-10.txt -i "mkdir tmp/gensort"
pssh -h hosts2-10.txt -i "tar -xvf tmp/gensort.tar.gz -C tmp/gensort"

# 4. 바이너리 이동
pssh -h hosts2-10.txt -i "mkdir .local"
pssh -h hosts2-10.txt -i "mkdir .local/bin"
pssh -h hosts2-10.txt -i "mv tmp/gensort/64/gensort .local/bin/gensort"

# 5. PATH 설정 (bashrc)
pssh -h hosts2-10.txt -i "echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"

# 6. PATH 설정 (bash_profile)
pssh -h hosts2-10.txt -i "echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bash_profile"

# 7. 설치 확인
pssh -h hosts2-10.txt -i "bash -l -c 'gensort'"
```

---

## 테스트 데이터 생성

### GenSort를 이용한 테스트 Input 생성

```bash
# 1. 테스트 디렉토리 생성
pssh -h hosts1-10.txt -i "mkdir shuffle_test_input"

# 2. 500개의 파티션 파일 생성 (각 100MB)
pssh -h hosts1-10.txt -t 0 -i "bash -l -c 'for file_num in \$(seq 1 500); do gensort -b 335544 shuffle_test_input/partition.\$file_num; done'"
```