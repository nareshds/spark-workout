package util

import util.CommandLineOptions.{Parser, Opt, NameValue}

/**
 * Created by ndharmasoth on 23-09-2015.
 */

case class  CommandLineOptions(programName: String, opts: Opt*){
  def helpMsg = s"""|usage: java ... $programName [options]
                   |where the options are the following:
                   |-h | --help  Show this message and quit.
                   |""".stripMargin + opts.map(_.help).mkString("\n")

  lazy val matchers: Parser =
    (opts foldLeft help) {
      (partialfunc, opt) => partialfunc orElse opt.parser
    } orElse noMatch

  protected def processOpts(args: Seq[String]): Seq[NameValue] =
    args match {
      case Nil => Nil
      case _ =>
        val (newArg, rest) = matchers(args)
        newArg +: processOpts(rest)
    }

  def apply(args: Seq[String]): Map[String,String] = {
    val foundOpts = processOpts(args)
    // Construct a map of the default args, then override with the actuals
    val map1 = opts.map(opt => opt.name -> opt.value).toMap
    // The actuals are already key-value pairs:
    val map2 = foundOpts.toMap
    val finalMap = map1 ++ map2
    if (finalMap.getOrElse("quiet", "false").toBoolean == false) {
      println(s"$programName:")
      finalMap foreach {
        case (key, value) => printf("  %15s: %s\n", key, value)
      }
    }
    finalMap
  }

  val help: Parser = {
    case ("-h" | "--h" | "--help") +: tail => quit("", 0)
  }

  val noMatch: Parser = {
    case head +: tail =>
      quit(s"Unrecognized argument (or missing second argument): $head", 1)
  }

  def quit(message: String, status: Int): Nothing = {
    if(message.length > 0) println(message)
    println(helpMsg)
    sys.exit(status)
  }
}


object CommandLineOptions {
  type NameValue = (String, String)
  type Parser = PartialFunction[Seq[String], (NameValue, Seq[String])]

  case class Opt(
                name: String,
                value: String,
                help: String,
                parser: Parser
                  )
  def inputPath(value: String): Opt =Opt(
    name = "input-path",
    value = value,
    help   = s"-i | --in  | --inpath  path   The input root directory of files to crawl (default: $value)",
    parser = {
      case ("-i" | "--in" | "--inpath") +: path +: tail => (("input-path", path), tail)
    })

  def outputPath(value: String): Opt = Opt(
    name   = "output-path",
    value  = value,
    help   = s"-o | --out | --outpath path   The output location (default: $value)",
    parser = {
      case ("-o" | "--out" | "--outpath") +: path +: tail => (("output-path", path), tail)
    })

  def master(value: String): Opt = Opt(
    name   = "master",
    value  = value,
    help   = s"""
                |-m | --master M      The "master" argument passed to SparkContext, "M" is one of:
                |                     "local", local[N]", "mesos://host:port", or "spark://host:port"
                |                     (default: $value).""".stripMargin,
    parser = {
      case ("-m" | "--master") +: master +: tail => (("master", master), tail)
    })

  def socket(hostPort: String): Opt = Opt(
    name   = "socket",
    value  = hostPort,
    help   = s"-s | --socket host:port  Listen to a socket for events (default: $hostPort unless --inpath used)",
    parser = {
      case ("-s" | "--socket") +: hp +: tail => (("socket", hp), tail)
    })

  def terminate(seconds: Int): Opt = Opt(
    name   = "terminate",
    value  = seconds.toString,
    help   = s"--term | --terminate N Terminate after N seconds. Use 0 for no termination.",
    parser = {
      case ("--term" | "--terminate") +: n +: tail => (("terminate", n), tail)
    })

  def quiet: Opt = Opt(
    name   = "quiet",
    value  = "false",
    help   = s"""-q | --quiet         Suppress some informational output.""",
    parser = {
      case ("-q" | "--quiet") +: tail => (("quiet", "true"), tail)
    })
}
