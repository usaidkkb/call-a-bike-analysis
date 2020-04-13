package config

import scopt.OptionParser

case class Config(input: String = "", output: String = "")

class OptionsParser extends OptionParser[Config]("Spark job config") {
  opt[String]('i', "input-file")
    .required()
    .action((value, arg) => arg.copy(input = value))
    .text("input csv path")

  opt[String]('o', "output-dir")
    .required()
    .action((value, arg) => arg.copy(output = value))
    .text("output directory path")
}
