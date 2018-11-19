package textanalyse

import org.apache.spark.AccumulatorParam

object VectorAccumulatorParam extends AccumulatorParam[Vector[Int]]{
  
   def zero(initialValue: Vector[Int]): Vector[Int] = {
     Vector.fill(initialValue.size){0}
   }
   
   def addInPlace(v1: Vector[Int], v2: Vector[Int]): Vector[Int] = {
     (for (i <- Range(0,(v1.size))) yield (v1(i)+v2(i))).toVector
   }
}