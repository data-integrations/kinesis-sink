/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch;

/**
 * Class to define property names for source and sinks
 */
public final class Properties {

  /**
   * Configuration for KinesisSink
   */
  public static class KinesisSink {
    public static final String NAME = "name";
    public static final String BODY_FIELD = "bodyField";
    public static final String ACCESS_ID = "accessID";
    public static final String ACCESS_KEY = "accessKey";
    public static final String DISTRIBUTE = "distribute";
    public static final String SHARD_COUNT = "shardCount";
  }

  /**
   * Common properties for BatchWritable source and sink
   */
  public static class BatchReadableWritable {
    public static final String NAME = "name";
    public static final String TYPE = "type";
  }

  private Properties() {
  }
}
