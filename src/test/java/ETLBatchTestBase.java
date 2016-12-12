/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.plugin.batch.KinesisOutputFormat;
import co.cask.hydrator.plugin.batch.KinesisSink;
import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.Set;

/**
 * Base test class that sets up plugin and the etl batch app artifacts.
 */
public class ETLBatchTestBase extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT, DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("core-plugins", "1.0.0"), parents,
                      KinesisSink.class,
                      KinesisOutputFormat.class);
  }
}
