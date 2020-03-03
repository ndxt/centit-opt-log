package com.centit.framework.test;

import com.centit.framework.system.po.EntOptLog;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.TableMapInfo;

public class TestSubClass {
    public static void main(String[] args) {
        TableMapInfo mapInfo =
            JpaMetadata.obtainMapInfoFromClass(EntOptLog.class);
        System.out.println(mapInfo.getPkName());

        EntOptLog log = new EntOptLog();
        //log.get
        mapInfo.setObjectFieldValue(log, "optTime", DatetimeOpt.currentUtilDate());

        System.out.println(log.getOptTime());
    }
}
