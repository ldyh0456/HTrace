/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class CustomSampler extends Sampler {
    private static final Log LOG = LogFactory.getLog(CustomSampler.class);
    private static String custom;
    private final static String SAMPLER_CUSTOM_CONF_KEY = "sampler.custom";
    private static Map<String, Double> map = new HashMap<>();

    public CustomSampler(HTraceConfiguration conf) {
        custom = conf.get(SAMPLER_CUSTOM_CONF_KEY);
        LOG.trace("Created new CustomSampler with custom = " + custom + ".");
        if (custom != null && !custom.equals("")) {
            split();
        }
    }

    private void split() {
        List<String> list = new ArrayList<String>();
        Collections.addAll(list, custom.split(","));
        for (String s : list) {
            String[] str = s.split(":");
            map.put(str[0], Double.parseDouble(str[1]));
        }
    }

    @Override
    public boolean next() {
        return true;
    }

    public boolean next(String description) {
        if (map.get(description) != null) {
            if (map.get(description) == 0) {
                return false;
            } else {
                return ThreadLocalRandom.current().nextDouble() < map.get(description);
            }
        }
        return true;
    }
}
