import java.io.{File, FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.{List, Set}

object Chang_Fan_SON {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Please input file path and name")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster("local").setAppName("Chang_Fan_SON")
    val sc = new SparkContext(conf)
    val case_num = args(0)
    val ratings = sc.textFile(args(1))
    val users = sc.textFile(args(2))
    val s = args(3)
    val n = 10 // # of chunk

    val ratings_3 = ratings.map(line => (line.split("::")(0).toInt, line.split("::")(1).toInt)) //(UID,MID)，key is UID
    //ratings_3.collect().foreach { x => println(x)}
    val users_3 = users.map(line => (line.split("::")(0).toInt, line.split("::")(1))) //(UID,gender), key is UID
    //users_3.collect().foreach { x => println(x)}
    val R_U = ratings_3.join(users_3) // (UID,(MID,Gender))
    //R_U.collect().foreach { x => println(x)}
    val MaleData = R_U.filter(line => line._2._2.contains("M"))
      .map(line => (line._1, line._2._1)).distinct().groupByKey()
      .map(line => line._2.toSet) // ((UID),MID)
    //MaleData.collect().foreach { x => println(x)}
    val FemaleData = R_U.filter(line => line._2._2.contains("F"))
      .map(line => (line._2._1, line._1)).distinct().groupByKey()
      .map(line => line._2.toSet) // (MID, UID) => (UID)
    //FemaleData.collect().foreach { x => println(x)}

    def Apriori(input_data: Iterator[Set[Int]]): Iterator[Set[Int]] = {
      //println("Call Apriori")
      val Threshold = s.toDouble / n
      var SingleItem: Set[Int] = Set()
      var AllSet: List[Set[Int]] = List()
      var AprResult: List[Iterable[Int]] = List()
      input_data.foreach { x => AllSet = AllSet :+ x; SingleItem = SingleItem ++ x }
      //AllSet.foreach(println)

      def SupportOf(itemComb: Set[Int]): Double = { // input a item set（eg.[1,2]）
        def withinAllSet(AllSets: Set[Int]): Boolean = {
          itemComb.map(x => AllSets.contains(x)).reduce((x1, x2) => x1 && x2) //check if the set within
        }

        val count = AllSet.count(withinAllSet) // is the set included in AllSet
        count.toDouble
      }

      val ItemCombs = SingleItem.map(item => (Set(item), SupportOf(Set(item)))) // （item，support）
        .filter(item_s => item_s._2 >= Threshold).map(item => item._1) // item > s
      AprResult = ItemCombs.toList.map(line => line.toList.toIterable).sortBy(line => line) // AprResult stores all L sets
      //println("check1" + ItemCombs)
      var currentLSet: Set[Set[Int]] = ItemCombs
      //println("check2" + currentLSet)
      var k: Int = 2
      while (currentLSet.nonEmpty) {
        val currentCSet: Set[Set[Int]] = currentLSet.map(Comb => currentLSet.map(Comb1 => Comb | Comb1)) // every set | with all other sets
          .reduce((Comb1, Comb2) => Comb1 | Comb2) // or again to remove duplicates
          .filter(Comb => Comb.size == k) // only remain k-size combs, those are Ck sets
        //println("check3" + currentCSet)
        val currentItemCombs = currentCSet.map(Comb => (Comb, SupportOf(Comb))) // get support of all Ck sets. currentItemCombs:(Comb, support)
          .filter(Comb_s => Comb_s._2 >= Threshold) // whether support > s
        //println("check4" + currentLSet)
        currentLSet = currentItemCombs.map(Comb_s => Comb_s._1) // remove support
        val temp = currentLSet.toList.map(line => line.toList.sorted).map(line => (line.toIterable, 1)).sortBy(line => line._1).map(line => line._1)
        AprResult = AprResult ++ temp
        k += 1
      }
      //      println("Frequent Sets of Single Chunk")
      //      AprResult.foreach(x => print(x)) // result of this chunk
      //      println("")
      AprResult.map(line => line.toSet).toIterator
    }

    def SON(candidates: Array[Set[Int]], all_data: RDD[Set[Int]]): Unit = {
      val name = "Chang_Fan_SON.case" + case_num.toString + "_" + s + ".txt"
      val writer = new FileWriter(new File(name))
      var AllData: Array[Set[Int]] = Array()
      all_data.collect().foreach(line => AllData = AllData :+ line)


      def SupportOf(itemComb: Set[Int]): Double = {
        def withinAllData(AllSets: Set[Int]): Boolean = {
          itemComb.map(x => AllSets.contains(x)).reduceRight((x1, x2) => x1 && x2)
        }

        val count = AllData.count(withinAllData) // AllData here is different with which in Apriori
        count.toDouble //
      }

      val currentLSet = candidates.map(Comb => (Comb, SupportOf(Comb))).filter(Comb_s => Comb_s._2 >= s.toDouble).map(Comb_s => Comb_s._1).distinct // filter using S
      //currentLSet.foreach(x => println(x))
      val SONResult = currentLSet.map(line => (line.toIterable, 1)).sortBy(line => line._1).map(line => line._1) // SONResult stores results of all sizes
      var ResultPerSize = SONResult.filter(line => line.size == 1) // add results of size 1, code below is used for sorting
      var size = 1
      var FormattedResult: List[List[String]] = List()
      while (ResultPerSize.nonEmpty) { // every time add all combs of one size
        var temp: List[Any] = List()
        ResultPerSize.foreach(pair => temp = temp :+ pair.toList) // turn to List
        val temp2 = temp.map(line => line.toString().substring(4, line.toString().length)) // to string and remove redundant string
        FormattedResult = FormattedResult :+ temp2 //List(List(single),List(double),List(triple))
        size += 1
        ResultPerSize = SONResult.filter(line => line.size == size)
      }
      val FinalResult = FormattedResult.map(line => line.toString().substring(5, line.toString().length - 1) + "\r") // remove redundant string
      FinalResult.foreach(x => println(x))
      FinalResult.foreach(x => writer.write(x) + System.lineSeparator() + writer.write("\r"))
      writer.close()
    }

    if (case_num == "1") {
      println("Case1")
      val Partitioned_MaleData = MaleData.repartition(n)
      val candidates = Partitioned_MaleData.mapPartitions(Apriori).collect()
      SON(candidates, MaleData)
    }
    else if (case_num == "2") {
      println("Case2")
      val Partitioned_FemaleData = FemaleData.repartition(n)
      val candidates = Partitioned_FemaleData.mapPartitions(Apriori).collect()
      SON(candidates, FemaleData)
    }
    else {
      println("Please input case 1 or 2")
    }
  }
}
