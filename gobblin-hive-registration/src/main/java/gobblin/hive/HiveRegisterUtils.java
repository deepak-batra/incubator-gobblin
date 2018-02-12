/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.hive;

import com.google.common.base.Preconditions;
import gobblin.configuration.State;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import gobblin.hive.spec.HiveSpec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * Utility class for registering data into Hive.
 */
public class HiveRegisterUtils {
  private static final String S3_ACCESS_KEY ="fs.s3a.access.key";
  private static final String S3_SECRET_KEY ="fs.s3a.secret.key";
  private static final String S3_PREFIX ="s3";
  private HiveRegisterUtils() {
  }

  /**
   * Register the given {@link Path}s.
   *
   * @param paths The {@link Path}s to be registered.
   * @param state A {@link State} which will be used to instantiate a {@link HiveRegister} and a
   * {@link HiveRegistrationPolicy} for registering the given The {@link Path}s.
   */
  public static void register(Iterable<String> paths, State state) throws IOException {
    try (HiveRegister hiveRegister = HiveRegister.get(state)) {
      HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(state);
      for (String path : paths) {
        for (HiveSpec spec : policy.getHiveSpecs(new Path(path))) {
          hiveRegister.register(spec);
        }
      }
    }
  }

  public  static String getTableLocation(HiveRegProps props, FileSystem fs, String dbName, String tableName){
    StringBuilder location = new StringBuilder();
    String hiveRegUri = props.getProp(HiveRegistrationPolicyBase.HIVE_FS_URI, "");
    Preconditions.checkArgument(!hiveRegUri.isEmpty(), HiveRegistrationPolicyBase.HIVE_FS_URI+ " cannot be left empty");
    if (!hiveRegUri.startsWith(S3_PREFIX))
      return fs.makeQualified(new Path(location.append(hiveRegUri).append(dbName).append(tableName).toString())).toString();

    String path = hiveRegUri.split(fs.getScheme() + "://")[1];
    location.append(fs.getScheme()).append("://").append(props.getSpecProperties().getProperty(S3_ACCESS_KEY)).append(":")
            .append(props.getSpecProperties().getProperty(S3_SECRET_KEY)).append("@").append(path).append(dbName).
            append("/").append(tableName);
    return location.toString();
  }

}
