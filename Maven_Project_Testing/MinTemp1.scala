//package uab.jjw.spark

// Importing the dependencies required
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.math.min
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WeatherMinimum extends Serializable {

  // Missing temparature to omit:
  val MISSING = 9999

  
  // main function
  def main(args:Array[String]) {

    // The main function starts here
    var dataFile = "/hduser/input/weather.txt"
    /*
    if(args.length > 0) {
      dataFile = args(0)
    }
    */
    /*
    val conf = new SparkConf()
    conf.setAppName("WeatherMinimumCalc");
    conf.set("spark.driver.allowMultipleContexts", "true");
    conf.setMaster("local[*]");
    */
    
    // Create a sparkcontext in local
    //val sc = new SparkContext(conf)

    //sc.addFile(dataFile, true)
        
    // Creating RDD of the data from file.
    val fs = sc.textFile(dataFile)
  
    // Using Map to extract out year and temperature fields only based on criteria
    val splitoutput = fs.map(line => {
      
    	// Extracting year info:
    	val year = line.substring(15, 19)
    	
      if(line.length() >= 94) {
      	// Calculating airTemperature:
      	val airTemperature = if (line.charAt(87) == '+') line.substring(88, 92) else line.substring(87, 92)

      	// Taking only those data having valid quality in "01459" and airTemperature is the Missing one:
      	val quality = line.substring(92, 93) 
      	if (Integer.parseInt(airTemperature) != MISSING && "01459".indexOf(quality) >= 0)  (year, airTemperature) else (year, Int.MaxValue.toString())
      }
    })
    
    // Remapping of extracted fields.
    val stationtemp = splitoutput.map({
      case (year, temperature) => year->temperature.asInstanceOf[String].toInt
      }) //mapping the year and temperature but in Integer value.

    // Finding out minimum temperature based on key (here year)
    val mintempstation = stationtemp.reduceByKey((x, y) => if(x < y) x else y) //reducing to minimum temperature by key

    // Now collecting is a List of results
    val results = mintempstation.collect() 
    
    //printing out the result
    for(result <- results)  println(s"(year, mintemp) is $result")
    
  }// end main
}//object


