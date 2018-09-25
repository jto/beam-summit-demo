package com.spotify

import com.spotify.scio._
import com.spotify.scio.bigquery._

/*
sbt runMain com.spotify.GitCommitsJob
  --runner=DataflowRunner
  --output=file:///Users/julient/Desktop/demo
  --tempLocation=gs://julient/temp
*/

object GitCommitsJob {

  @BigQueryType.fromQuery("""
    SELECT author
    FROM `bigquery-public-data.github_repos.commits`
    WHERE (
      SELECT COUNT(n)
      FROM UNNEST(repo_name) AS n
      WHERE n = 'spotify/scio'
    ) > 0
  """)
  class Github

  case class Args(output: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs.typed[Args](cmdlineArgs)

    sc.typedBigQuery[Github]()
      .flatMap { _.author.flatMap(_.name) }
      .countByValue
      .saveAsTextFile(args.output)

    val result = sc.close().waitUntilFinish()
  }
}
