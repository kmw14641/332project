package utils

import scopt.OParser

case class Config(numWorkers: Int = 0)

object MasterOptionUtils {
  private def getParser: OParser[Unit, Config] = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("master"),
        head("master", "1.0"),
        arg[Int]("<# of workers>")
          .required()
          .action((x, c) => c.copy(numWorkers = x))
          .text("number of workers to use")
          .validate(x =>
            if (x > 0) success
            else failure("number of workers must be natural number")
          )
      )
    }

    parser
  }

  def parse(args: Array[String]): Option[Int] = OParser.parse(getParser, args, Config()).map(_.numWorkers)
}
