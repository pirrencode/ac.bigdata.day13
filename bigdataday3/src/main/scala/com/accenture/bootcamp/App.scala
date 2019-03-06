package com.accenture.bootcamp

import org.apache.spark.rdd.RDD
import scala.language.postfixOps
import Utils._

object App extends SparkSupport {

  def unoptimalCode1(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty)

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head)

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2))

    // compute sizes of all groups
    val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }

    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2)

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  /*

Task #1
Elapsed time: 2027135662ns
( 35,11,36 ... 93,92,56)
Elapsed time: 673949616ns

The compared time after optimization, using cache, persist methods and reduceByKey give good results.
In my case the optimized code is 2.87 times faster. During experiments I get even better values.
However, I was confused with ReducedByKey syntax and called twice map and reduceByKey,
so if I'd manage to do without map - it would be even more faster.
When I run program for several times it brings


Task #2
Elapsed time: 697036216ns

After using aggregateByKey method in my case it returns result a little bit slower than reduceByKey.

Task #3
Elapsed time: 12309619717ns
Optimized
Elapsed time: 9065133553ns
After doing code optimization performance showed better results.

Conclusion:
a) The job done improved overall performance, due to the code optimization. Performance is key element in big data, so the result achieved.
b).cache + .persist are great combination, I used them on different cases and it returned amazing results.
1. There were real improve of performance after optimizations done by .cache.
2. The performance become even better when added .persist method.
3. After using reduceByKey the results become even better.
4. Using aggregateByKey didn't really change performance.
aggregateByKey() is almost identical to reduceByKey(), except we give a starting value for aggregateByKey(), that's why the difference isn't visible on this amount of data..

WEIRD ERROR: when I added this comments my code stopped working after starting to print. Task#3. It's already to late, maybe I removed a line, but it doesn't seem so, it worked before adding comment lines.

*/

  // TODO: Improve unoptimalCode1. Implement and test optimalCode1
  // Requirement: Write more time efficient code
  // Hint: you probably want to use reduceByKey.
  // Hint2: what about persistence?
  // Hint3: Any other way how to get results?
  // Hint4: use sc.parallelize() when providing data for tests
  def optimalCode1(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty).cache()

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head).persist()

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2)).persist()

    // compute sizes of all groups
    val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }.reduceByKey(_+_).cache()
    // { case (_, (crime, commitedCrime)) => (commitedCrime.district, crime.category) }.reduceByKey(_ + "," + _)

    // val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }.cache()


    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false).persist()
    // val sortedGroups = groupedCodes.reduceByKey(_+_)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2).persist()

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  // TODO: Rewrite optimalCode1 using aggregateByKey() instead of reduceByKey. Implement and test
  def optimalCode11(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty).cache()

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head).persist()

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2)).persist()


    // compute sizes of all groups
    val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }.aggregateByKey(0)((accumsy, value) => accumsy + 1, _ + _)
    // val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }.cache()


    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false).persist()
    // val sortedGroups = groupedCodes.reduceByKey(_+_)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2).persist()

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  def unoptimalCode2(crimesDb: RDD[String], commitedCrimes: RDD[String]): Unit = {

    case class Crime(code: String, code2: String, category: String, subcategory: String, level: String)
    case class CommitedCrime(cdatetime: String, address: String, district: String, beat: String, grid: String, crimedescr: String, ucr_ncic_code: String, latitude: String, longitude: String)

    // ignore empty lines
    val nonEmptyLines = crimesDb.filter(_.nonEmpty)

    // create RDD[Crime]
    val crimes = nonEmptyLines.map(line => {
      val cols = line.split("\t")
      Crime(cols(0), cols(1), cols(2), cols(3), cols(4))
    })

    var idx = 0

    // This function does processing and saving of data
    def addCommitedCrimes(commited: RDD[String]) = {

      // Map commited crimes with it's codes
      val codesCommited = commited.map(line => {
        val cols = line.split(",")
        // column 6 contains code
        (cols(6), CommitedCrime(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8)))
      })

      // combine each CommitedCrime with corresponding Crime by it's code
      val joinedCrimes = crimes.map(crime => (crime.code, crime)).join(codesCommited)

      // Store files in FS.
      joinedCrimes.map { case (_, (crime, commitedCrime)) => (commitedCrime.district, crime.category) }
        .reduceByKey(_ + "," + _)
        .saveAsTextFile("output/" + System.nanoTime() + "_output" + idx)
      idx += 1
    }

    // Code below simulates situation when new data comes in portions.
    // Think of it like each day you receive new data and need to process it and save the result
    val commitedCrimesParts = commitedCrimes.randomSplit(Array(.2, .2, .2, .2, .2))
    // 1st day data
    addCommitedCrimes(commitedCrimesParts(0))
    // 2nd day data
    addCommitedCrimes(commitedCrimesParts(1))
    // 3rd day data
    addCommitedCrimes(commitedCrimesParts(2))
    // 4th day data
    addCommitedCrimes(commitedCrimesParts(3))
    // 5th day data
    addCommitedCrimes(commitedCrimesParts(4))
  }

  // TODO: Improve unoptimalCode2. Implement and test optimalCode2
  // Requirement: Write more time efficient code
  // Hint: Use range partitioner
  // Hint1: Are there any other improvements?
  // Hint2: Do you need to persist something?

  def optimalCode2(crimesDb: RDD[String], commitedCrimes: RDD[String]): Unit = {

    // TODO: pre process crimesDB here
    case class Crime(code: String, code2: String, category: String, subcategory: String, level: String)
    case class CommitedCrime(cdatetime: String, address: String, district: String, beat: String, grid: String, crimedescr: String, ucr_ncic_code: String, latitude: String, longitude: String)

    // ignore empty lines
    val nonEmptyLines = crimesDb.filter(_.nonEmpty)

    // create RDD[Crime]
    val crimes = nonEmptyLines.map(line => {
      val cols = line.split("\t")
      Crime(cols(0), cols(1), cols(2), cols(3), cols(4))
    })

    var idx = 0
    // This function does processing and saving of data

    def addCommitedCrimes(commited: RDD[String]) = {

      // Map commited crimes with it's codes
      val codesCommited = commited.map(line => {
        val cols = line.split(",")
        // column 6 contains code
        (cols(6), CommitedCrime(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8)))
      })

      // TODO: join commitedCrimes with Crimes DB by code
      // TODO: for each district create list of categories of commited crimes
      // TODO: resulting RDD assign to result value

      val joinedCrimes = crimes.map(crime => (crime.code, crime)).join(codesCommited).cache()

      // Store files in FS.
      val result = joinedCrimes
      result.saveAsTextFile("output/" + System.nanoTime() + "_output" + idx)
      idx += 1
    }


    // DO NOT CHANGE CODE BELOW!
    // Code below simulates situation when new data comes in portions.
    // Think of it like each day you receive new data and need to process it and save the result
    val commitedCrimesParts = commitedCrimes.randomSplit(Array(.2, .2, .2, .2, .2))
    // 1st day data
    addCommitedCrimes(commitedCrimesParts(0))
    // 2nd day data
    addCommitedCrimes(commitedCrimesParts(1))
    // 3rd day data
    addCommitedCrimes(commitedCrimesParts(2))
    // 4th day data
    addCommitedCrimes(commitedCrimesParts(3))
    // 5th day data
    addCommitedCrimes(commitedCrimesParts(4))
  }

  def main(args: Array[String]): Unit = {

    // read text into RDD
    // Hint4: use sc.parallelize() when providing data for tests

    val crimeCategories = sc.textFile(filePath("ucr_ncic_codes.tsv"))

    println("Task #1")
    val (top, bottom) = time {
      unoptimalCode1(crimeCategories)
    }
    println(s"""( ${ top.mkString(",") } ... ${ bottom.reverse.mkString(",") })""")

    val commitedCrimes = sc.textFile(filePath("SacramentocrimeJanuary2006.csv"))

    time {
      optimalCode1(crimeCategories)
    }
    // TODO: check perfromance optimalCode1, optimalCode11
    println("Task #2")
    time {
      optimalCode11(crimeCategories)
    }
    // TODO: check performance unoptimalCode2, optimalCode2
    println("Task #3")
    time {
      unoptimalCode2(crimeCategories, commitedCrimes)
    }

    println("Optimized")
    time {
      optimalCode2(crimeCategories, commitedCrimes)
    }
  }
}