package org.camunda.tngp.broker.taskqueue.request.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.broker.taskqueue.CompleteTaskRequestReader;
import org.camunda.tngp.broker.taskqueue.MockTaskQueueContext;
import org.camunda.tngp.broker.taskqueue.TaskErrors;
import org.camunda.tngp.broker.taskqueue.TaskQueueContext;
import org.camunda.tngp.broker.taskqueue.log.TaskInstanceRequestReader;
import org.camunda.tngp.broker.test.util.BufferWriterUtil;
import org.camunda.tngp.broker.util.mocks.StubLogWriter;
import org.camunda.tngp.protocol.error.ErrorReader;
import org.camunda.tngp.protocol.taskqueue.CompleteTaskEncoder;
import org.camunda.tngp.protocol.log.TaskInstanceRequestType;
import org.camunda.tngp.transport.requestresponse.server.DeferredResponse;
import org.camunda.tngp.util.buffer.BufferWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CompleteTaskHandlerTest
{

    @Mock
    protected DirectBuffer message;

    @Mock
    protected DeferredResponse response;

    @Mock
    protected CompleteTaskRequestReader requestReader;

    @Captor
    protected ArgumentCaptor<BufferWriter> captor;

    protected TaskQueueContext taskContext;
    protected StubLogWriter logWriter;

    protected static final byte[] PAYLOAD = "cam".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        taskContext = new MockTaskQueueContext();

        logWriter = new StubLogWriter();
        taskContext.setLogWriter(logWriter);

    }


    @Test
    public void shouldWriteValidRequestToLog()
    {
        // given
        final CompleteTaskHandler handler = new CompleteTaskHandler();

        when(requestReader.consumerId()).thenReturn(765);
        when(requestReader.taskId()).thenReturn(123L);
        when(requestReader.getPayload()).thenReturn(new UnsafeBuffer(PAYLOAD));

        when(response.allocateAndWrite(any())).thenReturn(true, false);

        handler.requestReader = requestReader;

        // when
        handler.onRequest(taskContext, message, 123, 456, response);

        // then
        verify(response).deferFifo();
        verifyNoMoreInteractions(response);

        assertThat(logWriter.size()).isEqualTo(1);

        final TaskInstanceRequestReader logEntry = logWriter.getEntryAs(0, TaskInstanceRequestReader.class);
        assertThat(logEntry.consumerId()).isEqualTo(765L);
        assertThat(logEntry.type()).isEqualTo(TaskInstanceRequestType.COMPLETE);
        assertThat(logEntry.key()).isEqualTo(123L);
    }

    @Test
    public void shouldWriteErrorResponseOnMissingTaskId()
    {
        // given
        final CompleteTaskHandler handler = new CompleteTaskHandler();

        when(requestReader.consumerId()).thenReturn(765);
        when(requestReader.taskId()).thenReturn(CompleteTaskEncoder.taskIdNullValue());
        when(requestReader.getPayload()).thenReturn(new UnsafeBuffer(PAYLOAD));

        when(response.allocateAndWrite(any())).thenReturn(true, false);

        handler.requestReader = requestReader;

        // when
        handler.onRequest(taskContext, message, 123, 456, response);

        // then
        final InOrder inOrder = inOrder(response);
        inOrder.verify(response).allocateAndWrite(captor.capture());
        inOrder.verify(response).commit();
        verifyNoMoreInteractions(response);

        final ErrorReader reader = new ErrorReader();
        BufferWriterUtil.wrap(captor.getValue(), reader);

        assertThat(reader.componentCode()).isEqualTo(TaskErrors.COMPONENT_CODE);
        assertThat(reader.detailCode()).isEqualTo(TaskErrors.COMPLETE_TASK_ERROR);
        assertThat(reader.errorMessage()).isEqualTo("Task id is required");

        assertThat(logWriter.size()).isEqualTo(0);
    }

    @Test
    public void shouldWriteErrorResponseOnMissingConsumerId()
    {
        // given
        final CompleteTaskHandler handler = new CompleteTaskHandler();

        when(requestReader.consumerId()).thenReturn(CompleteTaskEncoder.consumerIdNullValue());
        when(requestReader.taskId()).thenReturn(123L);
        when(requestReader.getPayload()).thenReturn(new UnsafeBuffer(PAYLOAD));

        when(response.allocateAndWrite(any())).thenReturn(true, false);

        handler.requestReader = requestReader;

        // when
        handler.onRequest(taskContext, message, 123, 456, response);

        // then
        final InOrder inOrder = inOrder(response);
        inOrder.verify(response).allocateAndWrite(captor.capture());
        inOrder.verify(response).commit();
        verifyNoMoreInteractions(response);

        final ErrorReader reader = new ErrorReader();
        BufferWriterUtil.wrap(captor.getValue(), reader);

        assertThat(reader.componentCode()).isEqualTo(TaskErrors.COMPONENT_CODE);
        assertThat(reader.detailCode()).isEqualTo(TaskErrors.COMPLETE_TASK_ERROR);
        assertThat(reader.errorMessage()).isEqualTo("Consumer id is required");

        assertThat(logWriter.size()).isEqualTo(0);
    }
}
