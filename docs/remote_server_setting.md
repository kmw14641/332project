pip install --user pssh를 통해 master의 .local/bin에 pssh를 설치해놓고, ssh 핑거프린트를 20개의 worker에 (수작업으로 ㅠㅠ) 등록해놓음

pssh 사용법: host를 적어놓은 txt를 준비한 뒤 (~/hosts에 만들어놓음)
pssh -h hosts1.txt -i "ls"

배포 방법:
master에서 git clone을 받아 compile한 뒤, pscp를 사용하여 binary를 worker에 배포
pscp -h ~/hosts/hosts1-10.txt worker/target/scala-2.13/worker.jar /home/cyan
pssh -h ~/hosts/hosts1-10.txt -i "bash -l -c 'java -XX:MaxDirectMemorySize=2g -jar worker.jar > worker.log 2>&1'"

로그 보는 법:
ssh 접속 후 tail -f worker.log

고정포트 강제 종료:
pssh -h ~/hosts/hosts1-10.txt -i "lsof -t -i :8080 | xargs -r kill -9"

java 설치:
pssh -h ~/hosts/hosts2-10.txt -t 0 -i "curl -o tmp/openjdk17.tar.gz https://download.java.net/java/GA/jdk17/0d483333a00540d886896bac774ff48b/35/GPL/openjdk-17_linux-x64_bin.tar.gz"
pssh -h hosts2-10.txt -i "mkdir ~/.local/java"
pssh -h hosts2-10.txt -i "tar -xvf tmp/openjdk17.tar.gz -C ~/.local/java"
pssh -h hosts2-10.txt -i "echo 'export JAVA_HOME=\$HOME/.local/java/jdk-17' >> ~/.bashrc"
pssh -h hosts2-10.txt -i "echo 'export PATH=\$JAVA_HOME/bin:\$PATH' >> ~/.bashrc"
pssh -h hosts2-10.txt -i "echo 'export JAVA_HOME=\$HOME/.local/java/jdk-17' >> ~/.bash_profile"
pssh -h hosts2-10.txt -i "echo 'export PATH=\$JAVA_HOME/bin:\$PATH' >> ~/.bash_profile"
pssh -h hosts2-10.txt -i "bash -l -c 'java -version'"

gensort 설치:

pssh -h hosts2-10.txt -i "mkdir tmp"
pssh -h hosts2-10.txt -i "curl -o tmp/gensort.tar.gz https://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz"
pssh -h hosts2-10.txt -i "mkdir tmp/gensort"
pssh -h hosts2-10.txt -i "tar -xvf tmp/gensort.tar.gz -C tmp/gensort"
pssh -h hosts2-10.txt -i "mkdir .local"
pssh -h hosts2-10.txt -i "mkdir .local/bin"
pssh -h hosts2-10.txt -i "mv tmp/gensort/64/gensort .local/bin/gensort"
pssh -h hosts2-10.txt -i "echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
pssh -h hosts2-10.txt -i "echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bash_profile"
pssh -h hosts2-10.txt -i "bash -l -c 'gensort'"

gensort 테스트input 생성:
pssh -h hosts1-10.txt "mkdir shuffle_test_input"
pssh -h hosts1-10.txt -t 0 "bash -l -c 'for file_num in \$(seq 1 500); do gensort -b 335544 shuffle_test_input/partition.\$file_num; done'"