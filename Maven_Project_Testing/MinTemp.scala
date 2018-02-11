// Created by Jeremy Williams 

//package uab.jeremywork.spark

// Importing the dependencies required
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WeatherMinimum {

  // Missing temparature to omit:
  val MISSING = 9999

  // main function
  def main(args:Array[String]) {

    // The main function starts here
    
    // Setting the logger for errors
    Logger.getLogger("org").setLevel(Level.ERROR) 
                                                            
    // Create a sparkcontext in local
    val sc = new SparkContext("local[*]", "WeatherMinimum")
  
    // Creating RDD of the data from file.
    val fs = sc.textFile("/hduser/input/weather.txt")
  
    // Using Map to extract out year and temperature fields only based on criteria
    val splitoutput = fs.map(line => {
      if(line.length() >= 94) {
      	// Extracting year info:
      	val year = line.substring(15, 19)
      
      	// Calculating airTemperature:
      	val airTemperature = 
         	if (line.charAt(87) == '+') // parseInt doesn't like leading plus signs
        		line.substring(88, 92)
        	else
        		line.substring(87, 92)

      	(year, airTemperature)
        		
      	// Taking only those data having valid quality in "01459" and airTemperature is the Missing one:
      	val quality = line.substring(92, 93)
      	if (Integer.parseInt(airTemperature) != MISSING && "01459".indexOf(quality) >= 0)
      	  (year, airTemperature)
      	else (year, String.valueOf(Integer.MAX_VALUE))
      }              
    })
    
    // Remapping of extracted fields.
    val stationtemp = splitoutput.map({
      case (year, temperature) => year->temperature.asInstanceOf[String].toInt
      }) //mapping the year and temperature but in Integer value.

    // Finding out minimum temperature based on key 
    val mintempstation = stationtemp.reduceByKey((x, y) => if(x < y) x else y) //reducing to minimum temperature by key

    // Now collecting is a List of results
    val results = mintempstation.collect() //collecting the result
    
    for(result <- results) { //printing out the result in formatted way
      val year = result._1 // First field year
      val temperature = result._2 // Second field minimum temperature
      println(s"$year year has minimum temp: $temperature")
	  
    }// end for

  }// end main

}// end object
