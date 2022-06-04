package com.tvd12.ezymq.common.test.handler;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumers;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

@Test
public class EzyMQMessageConsumersTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        String command1 = RandomUtil.randomShortAlphabetString();
        String command3 = RandomUtil.randomShortAlphabetString();
        EzyMQMessageConsumer consumer1 = mock(EzyMQMessageConsumer.class);
        EzyMQMessageConsumer consumer2 = mock(EzyMQMessageConsumer.class);
        EzyMQMessageConsumer consumer3 = mock(EzyMQMessageConsumer.class);

        Object message = RandomUtil.randomShortAlphabetString();
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(consumer2).consume(message);

        // when
        EzyMQMessageConsumers sut = new EzyMQMessageConsumers();
        sut.addConsumer(command1, consumer1);
        sut.addConsumer(command1, consumer2);
        sut.addConsumer(command3, consumer3);
        sut.consume(command1, message);

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "consumers"),
            EzyMapBuilder.mapBuilder()
                .put(command1, Arrays.asList(consumer1, consumer2))
                .put(command3, Collections.singletonList(consumer3))
                .build(),
            false
        );
        verify(consumer1, times(1)).consume(message);
        verify(consumer2, times(1)).consume(message);
    }
}
