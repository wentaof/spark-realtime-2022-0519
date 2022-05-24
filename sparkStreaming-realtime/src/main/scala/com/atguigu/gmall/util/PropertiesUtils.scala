package com.atguigu.gmall.util

import java.util.ResourceBundle

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/23 14:58
  * @Version 1.0.0
  * @Description TODO
  */
object PropertiesUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key: String): String={
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtils("kafka.bootstrap-servers"))
  }
}
