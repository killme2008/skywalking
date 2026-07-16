/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.oap.server.core.analysis.manual.endpoint;

import org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor;
import org.apache.skywalking.oap.server.core.source.EndpointMeta;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

class EndpointMetaDispatcherTest {

    @Test
    void shouldUseMetadataTimeAsLastPing() {
        final MetricsStreamProcessor processor = mock(MetricsStreamProcessor.class);
        try (MockedStatic<MetricsStreamProcessor> mockedProcessor =
                 mockStatic(MetricsStreamProcessor.class)) {
            mockedProcessor.when(MetricsStreamProcessor::getInstance).thenReturn(processor);

            final EndpointMeta source = new EndpointMeta();
            source.setServiceName("service");
            source.setServiceNormal(true);
            source.setEndpoint("POST:/profile/{name}");
            source.setTimeBucket(202607161800L);
            source.prepare();

            new EndpointMetaDispatcher().dispatch(source);

            final ArgumentCaptor<EndpointTraffic> traffic =
                ArgumentCaptor.forClass(EndpointTraffic.class);
            verify(processor).in(traffic.capture());
            assertEquals(source.getTimeBucket(), traffic.getValue().getTimeBucket());
            assertEquals(source.getTimeBucket(), traffic.getValue().getLastPingTimestamp());
            assertEquals(source.getServiceId(), traffic.getValue().getServiceId());
            assertEquals(source.getEndpoint(), traffic.getValue().getName());
        }
    }
}
