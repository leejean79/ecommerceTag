package com.leejean.tagGen

//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
77287793	{"reviewPics":null,"extInfoList":[{"title":"contentTags","values":["干净卫生","服务热情"],"desc":"","defineType":0},{"title":"tagIds","values":["852","22"],"desc":"","defineType":0}],"expenseList":null,"reviewIndexes":[1,2],"scoreList":null}
 */

object TagGenerator {
  def main(args: Array[String])= {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TagGenerator by ***")
    val sc = new SparkContext(sparkConf)
//    val sqlContext = new HiveContext(sc)
//    import sqlContext.implicits._

    val poi_tags = sc.textFile("file:///E:\\tmp\\temptags.txt") // to be replaced by hive data resources

    //以tab进行切割
    val poi_taglist = poi_tags.map(e=>e.split("\t")).
    //第1次过滤
      filter(e=>e.length == 2).
    // 77287793->"干净卫生,服务热情"
      map(e=>e(0)->ReviewTags.extractTags(e(1))).
    // 77287793-> (干净卫生,服务热情)
      filter(e=> e._2.length > 0).map(e=> e._1 -> e._2.split(",")).
    // 77287793-> 干净卫生
     //      77287793  -> 服务热情
    flatMapValues(e=>e).
    //(77287793, 干净卫生) -> 1   (77287793, 服务热情) -> 1
      map(e=> (e._1,e._2)->1).
    // (77287793, 干净卫生) -> 23   (77287793, 服务热情) -> 41
      reduceByKey(_+_).
    // 77287793 -> list(干净卫生, 23)   77287793 -> list(服务热情, 41)
      map(e=> e._1._1 -> List((e._1._2,e._2))).
    // 77287793 -> list((干净卫生, 23), (服务热情, 41))
      reduceByKey(_:::_).
    // 77287793 -> list((服务热情, 41), (干净卫生, 23))
      map(e=> e._1-> e._2.sortBy(_._2).reverse.take(10).
//      77287793 -> 服务热情:41,干净卫生:23
        map(a=> a._1+":"+a._2.toString).mkString(","))
//    77287793  服务热情:41,干净卫生:23
    poi_taglist.map(e=>e._1+"\t"+e._2).saveAsTextFile("file:///E:\\tmp\\out")

    sc.stop()
  }
}
