import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.io.Source
var set = Set((1,2),(2,2),(1,4),(2,3))
var test = Set((1,3),(2,3),(1,2))

var list1 : Set[Set[Int]] = Set(Set(1,5),Set(2,4),Set(3,4))
var rdd1 = list1.map( wordSet => list1.map(wordSet1 => wordSet | wordSet1))
var rdd2 = list1.map( wordSet => list1.map(wordSet1 => wordSet | wordSet1)).reduce( (set1, set2) => set1 | set2)
var rdd22 = rdd2.toList.map(line =>line.toList)

var list3 = List(List(20,1,4),List(2,3,3),List(1,2,3),List(1036,1,2))
var list4 = List(List(5,6),List(3,4))
var list5 = list3.map(line => line.toString()).sorted.map(_.replaceAll("List", ""))
var list6 = list3.map(line => (line.toIterable, 1)).sortBy(x => x._1).map(line => line._1)

var list7 : List[Any] = List((1,2),2)
