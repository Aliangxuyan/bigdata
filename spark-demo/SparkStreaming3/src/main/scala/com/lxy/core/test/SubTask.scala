package com.lxy.core.test

/**
 * @author lxy
 * @date 2021/7/7
 */
class SubTask extends Serializable {
  var datas : List[Int] = _
  var logic : (Int)=>Int = _

  // 计算
  def compute() = {
    datas.map(logic)
  }
}
