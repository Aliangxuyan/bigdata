package com.lxy.core.test

/**
 * @author lxy
 * @date 2021/7/7
 */

class Task extends Serializable {

  val datas = List(1,2,3,4)

  //val logic = ( num:Int )=>{ num * 2 }
  val logic : (Int)=>Int = _ * 2


}
