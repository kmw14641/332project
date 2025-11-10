package utils

import scala.util.Try
import scopt.OParser
import java.net.InetAddress

case class WorkerConfig(
  masterAddr: String = "",
  inputDirectories: Seq[String] = Seq.empty,
  outputDirectory: String = ""
)

object WorkerOptionUtils {
  private def getParser: OParser[Unit, WorkerConfig] = {
    val builder = OParser.builder[WorkerConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("worker"),
        head("worker", "1.0"),
        arg[String]("<master ip:port>")
          .required()
          .action((x, c) => c.copy(masterAddr = x))
          .text("master address in format ip:port")
          .validate(x =>
            x.split(":") match {
              case Array(host, portStr) =>
                val portIsValid = Try(portStr.toInt).map(p => p > 0 && p < 65536).getOrElse(false)
                val hostIsValid = Try(InetAddress.getByName(host)).isSuccess
                if (hostIsValid && portIsValid) success
                else failure("master address must be a valid ip/host and port (1-65535)")
              case _ =>
                failure("master address must be in format ip:port")
            }
          ),
        opt[String]('I', "input")
          .required()
          .valueName("<dir>")
          .action((x, c) => c.copy(inputDirectories = x.split(" ").toSeq))
          .text("input directory"),
        opt[String]('O', "output")
          .required()
          .action((x, c) => c.copy(outputDirectory = x))
          .text("output directory")
          .validate(x =>
            if (x.nonEmpty) success
            else failure("output directory must not be empty")
          )
      )
    }

    parser
  }

  def parse(args: Array[String]): Option[(String, Seq[String], String)] = {
    val processedArgs = preprocessInputDirectoriesArgs(args)
    OParser.parse(getParser, processedArgs, WorkerConfig()).map {
      config => (config.masterAddr, config.inputDirectories, config.outputDirectory)
    }
  }
}
