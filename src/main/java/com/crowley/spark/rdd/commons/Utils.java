package com.crowley.spark.rdd.commons;

/**
 * Created by seroj on 9/14/17 6:04 PM.
 * Copyright (c) IUNetworks LLC.
 * All rights reserved.
 * This software is the confidential and proprietary information of IUNetworks LLC.
 * ("Confidential Information").  You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with IUNetworks LLC.
 */
public class Utils {
    private Utils(){
    };

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
}
