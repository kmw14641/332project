git fetch
git reset --hard origin/temp/remote-shuffle-test 
sbt worker/assembly
pssh -h ~/hosts/hosts1-10.txt -i "lsof -t -i :8080 | xargs -r kill -9"
pscp -h ~/hosts/hosts1-10.txt worker/target/scala-2.13/worker.jar /home/cyan
pssh -h ~/hosts/hosts1-10.txt -i "bash -l -c 'java -XX:MaxDirectMemorySize=10g -jar worker.jar > worker.log 2>&1'"