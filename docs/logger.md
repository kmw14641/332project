현재 println으로 로그를 찍고 있음. println은 요구사항의 출력만 담당하고, 흐름로그/디버그는 파일로 남기게 해야 함.

gRPC 로그 남기는 법 (별로 도움이 안됐어서 코드에는 안올림):

```properties
# 1. ConsoleHandler와 FileHandler를 모두 사용
handlers=java.util.logging.FileHandler

# 3. FileHandler 설정 (핵심!)
java.util.logging.FileHandler.pattern=grpc.log
java.util.logging.FileHandler.level=ALL
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter

# 4. gRPC 로거 레벨 설정
.level = INFO
io.grpc.netty.NettyServerHandler.level = FINE
```

logger.properties에 저장한 뒤, java 실행 시 다음 옵션 추가:
```bash
-Djava.util.logging.config.file=.logger.properties
```