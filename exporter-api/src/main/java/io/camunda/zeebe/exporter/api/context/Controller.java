/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.exporter.api.context;

import java.time.Duration;
import java.util.Optional;

/** Controls various aspect of the exporting process. */
public interface Controller {
  /**
   * Signals to the broker that the exporter has successfully exported all records up to and
   * including the record at {@param position}.
   *
   * @param position the latest successfully exported record position
   */
  void updateLastExportedRecordPosition(final long position);

  /**
   * Signals to the broker that the exporter has successfully exported all records up to and
   * including the record at {@param position}. Optionally, the exporter can provide arbitrary
   * metadata that is stored in the broker and can be retrieved when the exporter opens. The
   * metadata is stored only if the given position is greater than the previous exporter position.
   *
   * @param position the latest successfully exported record position
   * @param metadata arbitrary metadata for the exporter (can be {@code null})
   */
  void updateLastExportedRecordPosition(long position, byte[] metadata);

  /**
   * Schedules a cancellable {@param task} to be ran after {@param delay} has expired.
   *
   * @param delay time to wait until the task is ran
   * @param task the task to run
   * @return cancellable task.
   */
  ScheduledTask scheduleCancellableTask(final Duration delay, final Runnable task);

  /**
   * Read arbitrary metadata of the exporter that was stored previously by using the exporter
   * controller.
   *
   * @return the restored metadata, or {@link Optional#empty()} if no metadata exist
   */
  Optional<byte[]> readMetadata();
}
