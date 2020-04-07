/*
 * Copyright © 2019 CDAP
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

package io.cdap.plugin.batch;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("KinesisSink")
@Description("Sink that outputs to a specified AWS Kinesis stream.")
public class KinesisSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Text> {

  private static final String NULL_STRING = "\0";
  private static final Integer DEFAULT_SHARD_COUNT = 1;

  private final KinesisConfig config;

  public KinesisSink(KinesisConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    FailureCollector collector = batchSinkContext.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    batchSinkContext.addOutput(Output.of(config.referenceName, new KinesisOutputFormatProvider(config)));

    Schema schema = batchSinkContext.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(batchSinkContext, config.referenceName, schema, "Write", "Wrote to Kinesis Stream");
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    List<String> dataArray = new ArrayList<>();
    for (Schema.Field field : input.getSchema().getFields()) {
      Object fieldValue = input.get(field.getName());
      String data = (fieldValue != null) ? fieldValue.toString() : NULL_STRING;
      dataArray.add(data);
    }
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(Joiner.on(",").join(dataArray))));
  }

  private static class KinesisOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    private KinesisOutputFormatProvider(KinesisConfig config) {
      this.conf = new HashMap<>();
      conf.put(Properties.KinesisSink.ACCESS_ID, config.awsAccessKey);
      conf.put(Properties.KinesisSink.ACCESS_KEY, config.awsAccessSecret);
      conf.put(Properties.KinesisSink.SHARD_COUNT, String.valueOf(config.getShardCount()));
      conf.put(Properties.KinesisSink.DISTRIBUTE, config.getDistribute());
      conf.put(Properties.KinesisSink.NAME, config.name);
    }

    @Override
    public String getOutputFormatClassName() {
      return KinesisOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  /**
   * config file for Kinesis stream sink
   */
  public static class KinesisConfig extends ReferencePluginConfig {

    @Description("The name of the Kinesis stream to output to. The stream will be created if it does not exist.")
    private String name;

    @Name(Properties.KinesisSink.ACCESS_ID)
    @Description("AWS access Id having access to Kinesis streams")
    @Macro
    private String awsAccessKey;

    @Name(Properties.KinesisSink.ACCESS_KEY)
    @Description("AWS access key secret having access to Kinesis streams")
    @Macro
    private String awsAccessSecret;

    @Name(Properties.KinesisSink.SHARD_COUNT)
    @Description("Number of shards to be created, each shard has throughput of 1mb/s")
    @Macro
    @Nullable
    private Integer shardCount;

    @Name(Properties.KinesisSink.DISTRIBUTE)
    @Description("Boolean to decide if the data has to be uniformly distributed among all the shards or has to be " +
      "sent to a single shard.")
    private String distribute;

    public KinesisConfig(String referenceName, String name, String awsAccessKey,
                         String awsAccessSecret, Integer shardCount, String distribute) {
      super(referenceName);
      this.name = name;
      this.awsAccessKey = awsAccessKey;
      this.awsAccessSecret = awsAccessSecret;
      this.shardCount = shardCount;
      this.distribute = distribute;
    }

    public Integer getShardCount() {
      return shardCount == null ? DEFAULT_SHARD_COUNT : shardCount;
    }


    public String getDistribute() {
      //ensuring that distribute can have only 2 possible values "true" or "false". defaults to true
      return Boolean.toString(Boolean.parseBoolean(distribute));
    }

    private void validate(FailureCollector collector) {
      if (Strings.isNullOrEmpty(name)) {
        collector.addFailure("Stream name should be non-null, non-empty.", null)
          .withConfigProperty(Properties.KinesisSink.NAME);
      }
      if (!containsMacro(Properties.KinesisSink.ACCESS_ID) && Strings.isNullOrEmpty(awsAccessKey)) {
        collector.addFailure("Access Key should be non-null, non-empty.", null)
          .withConfigProperty(Properties.KinesisSink.ACCESS_ID);
      }
      if (!containsMacro(Properties.KinesisSink.ACCESS_KEY) && Strings.isNullOrEmpty(awsAccessSecret)) {
        collector.addFailure("Access Key secret should be non-null, non-empty.", null)
          .withConfigProperty(Properties.KinesisSink.ACCESS_KEY);
      }
    }
  }
}
