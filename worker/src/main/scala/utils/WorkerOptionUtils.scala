package utils

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
            if (
              x.split(":").length == 2 &&
              scala.util.Try(InetAddress.getByName(x.split(":")(0))).isSuccess &&
              scala.util.Try(x.split(":")(1).toInt).isSuccess &&
              0 < x.split(":")(1).toInt && x.split(":")(1).toInt < 65536
            ) success
            else failure("master address must be in format ip:port")
          ),
        opt[Seq[String]]('I', "input")
          .required()
          .unbounded()
          .valueName("<dir1> <dir2>...")
          .action((x, c) => c.copy(inputDirectories = c.inputDirectories ++ x))
          .text("input directories")
          .validate(x =>
            if (x.nonEmpty) success
            else failure("at least one input directory must be specified")
          ),
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

  def parse(args: Array[String]): Option[(String, Seq[String], String)] = OParser.parse(getParser, args, WorkerConfig()).map {
    config => (config.masterAddr, config.inputDirectories, config.outputDirectory)
  }
}
