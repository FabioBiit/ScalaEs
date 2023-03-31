import scala.util.control.Breaks.break

object N_primi extends App {

  val num = 100
  var count = 0

  for(i<- 1 to num){
    count = 0
    for(j<- 2 to i/2){
      if( i%j == 0 ){
        count += 1
      }
    }
    if(count == 0){
      print(i+" ")
    }
  }

 /* println(" ")

  val num1 = 15
  var primo = true

  for(i<- 1 to num1){
    primo = true
    for(j<- 2 until i ){
      if(i%j==0){
        primo = false
      }
    }
    if(primo == true) {
      print(i + " ")
    }

  }*/


}
