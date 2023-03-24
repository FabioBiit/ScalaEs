

object ArraySort extends App {

  var arr1 = Array(0, 1, 11, 10, 3, 934, 2, 7, 5, 1256, 4, 9, 11, 10, 10, 8, 6, 123)

  val size1 = arr1.length

  for(i <- 0 to size1-1){

    val index = i

    //println(i)

    for(j <- 0 to size1-1){

      if( arr1(index) < arr1(j) ) {

        val temp = arr1(j)
        arr1(j) = arr1(index)
        arr1(index) = temp

      }

    }

  }

  //arr1.foreach(println)

  var arr = Array(0, 1, 3, 2, 7, 5, 4, 9, 10, 8)

  val n = arr.length

  for (i <- 0 until n - 1) {
    for (j <- 0 until n - i - 1) {
      if (arr(j) > arr(j + 1)) {
        val temp = arr(j)
        arr(j) = arr(j + 1)
        arr(j + 1) = temp
      }
    }
  }

  var x = " "

  for(n <- 0 until  5){

    x += "*"

    println(x)

  }

  //arr.foreach(println)

}
