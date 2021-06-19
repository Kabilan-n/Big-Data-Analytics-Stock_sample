//19AIE214 Big Data Analytics
//Assignments

//Date: May 2 2021

//Kabilan N
//CB.EN.U4AIE19033

// 4. Use the ‘nsesample.txt’ trades file and find out the following.


// a) How many trades happened on July 1st 2014 (1st field)

val nseRdd = sc.textFile("Data//nsesample.txt")
// nseRdd: org.apache.spark.rdd.RDD[String] = Data//nsesample.txt MapPartitionsRDD[56] at textFile at <console>:24

val nseSchemaRdd = nseRdd.map { l =>
	val str0 = l.split('|')
    val (ds, ts, id, stck) = (str0(0).toInt, str0(1), str0(2).toInt, str0(3)  )
    val (pr, vol) = (str0(4).toFloat, str0(5).toFloat )
    (ds, ts, id, stck, pr, vol ) }
// nseSchemaRdd: org.apache.spark.rdd.RDD[(Int, String, Int, String, Float, Float)] = MapPartitionsRDD[57] at map at <console>:26

nseSchemaRdd.toDF.show()
/*+--------+-----+-------+----------+-------+-------+
|      _1|   _2|     _3|        _4|     _5|     _6|
+--------+-----+-------+----------+-------+-------+
|20140701|09:07|4336224|  AXISBANK| 1928.0|  768.0|
|20140701|09:07|   1064|BHARTIARTL| 337.25| 2625.0|
|20140701|09:07|    642|      BHEL|  251.9| 4312.0|
|20140701|09:07|   3459|   HCLTECH| 1495.0|  838.0|
|20140701|09:07|   2972|      HDFC| 990.25|  880.0|
|20140701|09:07|   3121|  HINDALCO|  167.2|46978.0|
|20140701|09:07|2600758| ICICIBANK|1420.55| 1113.0|
|20140701|09:07|2601408|      IDEA|  133.1| 3360.0|
|20140701|09:07|2600340|      INFY| 3245.0|  513.0|
|20140701|09:07|2600578|       ITC|  327.6| 9406.0|
|20140701|09:07|2602073|        LT| 1712.0| 1391.0|
|20140701|09:07|2602835|    MARUTI| 2453.8|  315.0|
|20140701|09:07|4335840|      ONGC|  429.0| 8920.0|
|20140701|09:07|4336599| POWERGRID|  140.0| 9231.0|
|20140701|09:07|4336134|  RELIANCE| 1017.2| 5277.0|
|20140701|09:07|4337085|      SBIN| 2699.0| 1057.0|
|20140701|09:07|4337271| SUNPHARMA| 686.55| 1657.0|
|20140701|09:07|4337482|TATAMOTORS|  432.8| 2332.0|
|20140701|09:07|4338209|       TCS| 2418.9|  657.0|
|20140701|09:07|2602904|   YESBANK|  543.7| 4864.0|
+--------+-----+-------+----------+-------+-------+
only showing top 20 rows*/



val DateReducedRdd = nseSchemaRdd.map { case (ds, ts, id, stck, pr, vol ) =>
    (ds,List((stck))) }.reduceByKey(_ ++ _)
// DateReducedRdd: org.apache.spark.rdd.RDD[(Int, List[String])] = ShuffledRDD[68] at reduceByKey at <console>:29

DateReducedRdd.toDF.show()
/*+--------+--------------------+
|      _1|                  _2|
+--------+--------------------+
|20140702|[AXISBANK, BHARTI...|
|20140703|[AXISBANK, BHARTI...|
|20140701|[AXISBANK, BHARTI...|
+--------+--------------------+
*/

val TradeOnJul_1=DateReducedRdd.filter(_._1 == 20140701)
// TradeOnJul_1: org.apache.spark.rdd.RDD[(Int, List[String])] = MapPartitionsRDD[72] at filter at <console>:30

TradeOnJul_1.map { case (ds,list) => (ds, list.length) }.toDF.show()
/*+--------+----+
|      _1|  _2|
+--------+----+
|20140701|7703|
+--------+----+
*/


// b) How many companies traded on the above date.

val UniqueCompanyRdd = TradeOnJul_1.map { case(ds,list) => (ds, list.toSet.size) }
// UniqueCompanyRdd: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[77] at map at <console>:32

UniqueCompanyRdd.toDF.show()
/*+--------+---+
|      _1| _2|
+--------+---+
|20140701| 20|
+--------+---+*/


nseSchemaRdd.toDF.show()
/*+--------+-----+-------+----------+-------+-------+
|      _1|   _2|     _3|        _4|     _5|     _6|
+--------+-----+-------+----------+-------+-------+
|20140701|09:07|4336224|  AXISBANK| 1928.0|  768.0|
|20140701|09:07|   1064|BHARTIARTL| 337.25| 2625.0|
|20140701|09:07|    642|      BHEL|  251.9| 4312.0|
|20140701|09:07|   3459|   HCLTECH| 1495.0|  838.0|
|20140701|09:07|   2972|      HDFC| 990.25|  880.0|
|20140701|09:07|   3121|  HINDALCO|  167.2|46978.0|
|20140701|09:07|2600758| ICICIBANK|1420.55| 1113.0|
|20140701|09:07|2601408|      IDEA|  133.1| 3360.0|
|20140701|09:07|2600340|      INFY| 3245.0|  513.0|
|20140701|09:07|2600578|       ITC|  327.6| 9406.0|
|20140701|09:07|2602073|        LT| 1712.0| 1391.0|
|20140701|09:07|2602835|    MARUTI| 2453.8|  315.0|
|20140701|09:07|4335840|      ONGC|  429.0| 8920.0|
|20140701|09:07|4336599| POWERGRID|  140.0| 9231.0|
|20140701|09:07|4336134|  RELIANCE| 1017.2| 5277.0|
|20140701|09:07|4337085|      SBIN| 2699.0| 1057.0|
|20140701|09:07|4337271| SUNPHARMA| 686.55| 1657.0|
|20140701|09:07|4337482|TATAMOTORS|  432.8| 2332.0|
|20140701|09:07|4338209|       TCS| 2418.9|  657.0|
|20140701|09:07|2602904|   YESBANK|  543.7| 4864.0|
+--------+-----+-------+----------+-------+-------+
only showing top 20 rows*/


// c) No: of trades between 9:00 and 10:00 throughout all days.

val TimeReducedRdd = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) =>
    val s = ts.split(":")
    val t = s(0)+s(1)
    (t.toInt, id, stck)}
// TimeReducedRdd: org.apache.spark.rdd.RDD[(Int, Int, String)] = MapPartitionsRDD[84] at map at <console>:28

TimeReducedRdd.toDF.show()
/*+---+-------+----------+
| _1|     _2|        _3|
+---+-------+----------+
|907|4336224|  AXISBANK|
|907|   1064|BHARTIARTL|
|907|    642|      BHEL|
|907|   3459|   HCLTECH|
|907|   2972|      HDFC|
|907|   3121|  HINDALCO|
|907|2600758| ICICIBANK|
|907|2601408|      IDEA|
|907|2600340|      INFY|
|907|2600578|       ITC|
|907|2602073|        LT|
|907|2602835|    MARUTI|
|907|4335840|      ONGC|
|907|4336599| POWERGRID|
|907|4336134|  RELIANCE|
|907|4337085|      SBIN|
|907|4337271| SUNPHARMA|
|907|4337482|TATAMOTORS|
|907|4338209|       TCS|
|907|2602904|   YESBANK|
+---+-------+----------+
only showing top 20 rows*/


val tradebw9t10 = TimeReducedRdd.filter(_._1 <= 1000)
// tradebw9t10: org.apache.spark.rdd.RDD[(Int, Int, String)] = MapPartitionsRDD[88] at filter at <console>:30

tradebw9t10.count()
// res13: Long = 2820


// d) How many total companies traded and how much per stock.

val stckReducedRdd = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) =>
    (stck,Array(pr*vol)) }.reduceByKey( _ ++ _)
// stckReducedRdd: org.apache.spark.rdd.RDD[(String, Array[Float])] = ShuffledRDD[90] at reduceByKey at <console>:29

stckReducedRdd.toDF.show()
/*+----------+--------------------+
|        _1|                  _2|
+----------+--------------------+
|       TCS|[1589217.2, 1.673...|
|      INFY|[1664685.0, 1.779...|
|        LT|[2381392.0, 4.976...|
|    MARUTI|[772947.0, 491936...|
|  HINDALCO|[7854721.5, 6.337...|
|TATAMOTORS|[1009289.56, 1.90...|
|      SBIN|[2852843.0, 3.573...|
|       ITC|[3081405.8, 6.789...|
|   HCLTECH|[1252810.0, 1.514...|
| ICICIBANK|[1581072.2, 4.035...|
| POWERGRID|[1292340.0, 2.481...|
|      ONGC|[3826680.0, 5.718...|
|  AXISBANK|[1480704.0, 1.516...|
|      HDFC|[871420.0, 3.1145...|
|      BHEL|[1086192.8, 1.598...|
|  RELIANCE|[5367764.5, 4.746...|
|BHARTIARTL|[885281.25, 1.361...|
|   YESBANK|[2644556.8, 2.780...|
|      IDEA|[447216.03, 42777...|
| SUNPHARMA|[1137613.4, 92018...|
+----------+--------------------+
*/

val volTotalRdd = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) => vol}.sum()
// volTotalRdd: Double = 2.0424428E8

val CompanyPrice = stckReducedRdd.map{ case(stck,array) =>
    val avg = (array.sum)/volTotalRdd
    (stck, avg)}
// CompanyPrice: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[96] at map at <console>:32

CompanyPrice.foreach(println)
/*(TCS,16.644162294287998)
(HCLTECH,18.00409493964776)
(INFY,29.75769179925137)
(ICICIBANK,48.081843956658176)
(LT,31.68161481927425)
(POWERGRID,15.381240385287656)
(ONGC,29.7874450339564)
(MARUTI,32.243569670592485)
(AXISBANK,20.78466745800666)
(HINDALCO,30.894025213337677)
(HDFC,21.061565063168477)
(TATAMOTORS,38.88422643708798)
(BHEL,21.18463902146978)
(SBIN,47.71438687046707)
(RELIANCE,32.325669947770386)
(ITC,30.20098967765462)
(BHARTIARTL,11.587017350008528)
(YESBANK,25.92024397451914)
(IDEA,13.948767152744743)
(SUNPHARMA,19.113204482397254)*/

// e) Create a DF of data set and show the companies and prices

CompanyPrice.toDF.show()
/*+----------+------------------+
|        _1|                _2|
+----------+------------------+
|       TCS|16.644162294287998|
|      INFY| 29.75769179925137|
|        LT| 31.68161481927425|
|    MARUTI|32.243569670592485|
|  HINDALCO|30.894025213337677|
|TATAMOTORS| 38.88422643708798|
|      SBIN| 47.71438687046707|
|       ITC| 30.20098967765462|
|   HCLTECH| 18.00409493964776|
| ICICIBANK|48.081843956658176|
| POWERGRID|15.381240385287656|
|      ONGC|  29.7874450339564|
|  AXISBANK| 20.78466745800666|
|      HDFC|21.061565063168477|
|      BHEL| 21.18463902146978|
|  RELIANCE|32.325669947770386|
|BHARTIARTL|11.587017350008528|
|   YESBANK| 25.92024397451914|
|      IDEA|13.948767152744743|
| SUNPHARMA|19.113204482397254|
+----------+------------------+*/


// f) Amalgamate the data with an hourly time stamp (use ‘reduceByKey’); average the price and add the volumes, write it to a directory (use rdd.saveAsTextFile(“directory”) ).

val Totalcnt = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) =>
    val tsd = ts.split(":")
    ((ds,tsd(0)),1)}.reduceByKey(_+_).sortBy(_._1)
// Totalcnt: org.apache.spark.rdd.RDD[((Int, String), Int)] = MapPartitionsRDD[106] at sortBy at <console>:30


val TotalPrice = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) =>
    val tsd = ts.split(":")
    ((ds,tsd(0)),pr)}.reduceByKey(_+_).sortBy(_._1)
// TotalPrice: org.apache.spark.rdd.RDD[((Int, String), Float)] = MapPartitionsRDD[113] at sortBy at <console>:30

val Totalvol = nseSchemaRdd.map { case(ds, ts, id, stck, pr, vol ) =>
    val tsd = ts.split(":")
    ((ds,tsd(0)),vol)}.reduceByKey(_+_).sortBy(_._1)
// Totalvol: org.apache.spark.rdd.RDD[((Int, String), Float)] = MapPartitionsRDD[120] at sortBy at <console>:30

val Total = ((Totalcnt join TotalPrice  )join Totalvol).sortBy(_._1)
// Total: org.apache.spark.rdd.RDD[((Int, String), ((Int, Float), Float))] = MapPartitionsRDD[131] at sortBy at <console>:34

val DFRdd = Total.map{case((ds, tm), ((cnt, pr), vol))=> ((ds,tm),pr/cnt,vol)}
// DFRdd: org.apache.spark.rdd.RDD[((Int, String), Float, Float)] = MapPartitionsRDD[132] at map at <console>:36

DFRdd.toDF("date and time","Avg Price","Total Vol / hour").show()
/*+-------------+---------+----------------+
|date and time|Avg Price|Total Vol / hour|
+-------------+---------+----------------+
|[20140701,09]|1141.9519|     2.0161176E7|
|[20140701,10]|1142.1617|     1.1876386E7|
|[20140701,11]|1150.6204|       8235269.0|
|[20140701,12]|1145.8376|       8264323.0|
|[20140701,13]|1145.2273|       8388763.0|
|[20140701,14]|1146.7056|     1.1461912E7|
|[20140701,15]|1111.5363|     1.2031596E7|
|[20140702,09]|1152.0974|     1.6008942E7|
|[20140702,10]|1155.4465|     1.2055746E7|
|[20140702,11]|1160.5166|       8317887.0|
|[20140702,12]|1154.8484|       8471638.0|
|[20140702,13]|   1156.2|       9923698.0|
|[20140702,14]|1156.1263|     1.1082862E7|
|[20140702,15]|1130.4727|     1.5314167E7|
|[20140703,09]|1158.9916|     1.4552694E7|
|[20140703,10]|1158.3129|       8909720.0|
|[20140703,11]|1158.6658|     1.0794019E7|
|[20140703,12]|1158.3597|       7877229.0|
|[20140703,13]| 1146.532|        516233.0|
+-------------+---------+----------------+
*/

