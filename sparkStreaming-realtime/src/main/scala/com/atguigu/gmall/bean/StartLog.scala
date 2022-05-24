package com.atguigu.gmall.bean

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/23 16:03
  * @Version 1.0.0
  * @Description TODO
  */
case class StartLog(
         mid :String,
         user_id:String,
         province_id:String,
         channel:String,
         is_new:String,
         model:String,
         operate_system:String,
         version_code:String,
         brand : String ,
         entry:String,
         open_ad_id:String,
         loading_time_ms:Long,
         open_ad_ms:Long,
         open_ad_skip_ms:Long,
         ts:Long
                   ) {

}
