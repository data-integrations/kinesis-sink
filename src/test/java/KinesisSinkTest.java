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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.batch.Properties;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for Kinesis batch sink
 */
public class KinesisSinkTest extends ETLBatchTestBase {

  /**
   * Method to test the kinesis client creation
   *
   * @throws Exception
   */
  @Test
  public void testKinesisSink() throws Exception {

    Schema schema = Schema.recordOf(
      "action",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT))
    );

    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", "KinesisSinkTest");
    properties.put(Properties.KinesisSink.NAME, "unitTest");
    properties.put(Properties.KinesisSink.ACCESS_ID, "someId");
    properties.put(Properties.KinesisSink.ACCESS_KEY, "SomeSecret");
    properties.put(Properties.KinesisSink.BODY_FIELD, "body");
    properties.put(Properties.KinesisSink.SHARD_COUNT, "1");
    properties.put(Properties.KinesisSink.DISTRIBUTE, "true");

    ETLPlugin sinkConfig = new ETLPlugin("KinesisSink", BatchSink.PLUGIN_TYPE, properties, null);
    ETLPlugin source2 = MockSource.getPlugin("KinesisSinkInputTable", schema);

    ETLStage source = new ETLStage("source", source2);
    ETLStage sink = new ETLStage("sink", sinkConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KinesisSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    Assert.assertEquals(workflowManager.getHistory().size(), 1);
    Assert.assertEquals(workflowManager.getHistory().get(0).getStatus(), ProgramRunStatus.FAILED);
  }
}
