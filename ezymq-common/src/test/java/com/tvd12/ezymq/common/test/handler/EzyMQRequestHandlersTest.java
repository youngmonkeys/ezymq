package com.tvd12.ezymq.common.test.handler;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandlers;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzyMQRequestHandlersTest extends BaseTest {

    @Test
    public void test() throws Exception {
        // given
        EzyTestMQRequestHandler handler1 = mock(EzyTestMQRequestHandler.class);
        EzyTestMQRequestHandler handler2 = mock(EzyTestMQRequestHandler.class);
        EzyTestMQRequestHandler handler3 = mock(EzyTestMQRequestHandler.class);

        InternalHandlers sut = new InternalHandlers();
        String command1 = RandomUtil.randomShortAlphabetString();
        String command2 = RandomUtil.randomShortAlphabetString();
        String command3 = RandomUtil.randomShortAlphabetString();
        sut.addHandler(command1, handler1);
        sut.addHandlers(
            EzyMapBuilder.mapBuilder()
                .put(command2, handler2)
                .put(command3, handler3)
                .toMap()
        );
        Object request = RandomUtil.randomShortAlphabetString();
        Object result = RandomUtil.randomShortAlphabetString();
        when(handler1.handle(request)).thenReturn(result);

        // when
        Object actual = sut.handle(command1, request);

        // then
        Asserts.assertEquals(actual, result);
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "handlers"),
            EzyMapBuilder.mapBuilder()
                .put(command1, handler1)
                .put(command2, handler2)
                .put(command3, handler3)
                .toMap(),
            false
        );
    }

    @Test
    public void testWithException() {
        // given
        InternalHandlers sut = new InternalHandlers();
        String command1 = RandomUtil.randomShortAlphabetString();

        // when
        Object request = RandomUtil.randomShortAlphabetString();
        Throwable e = Asserts.assertThrows(() ->
            sut.handle(command1, request)
        );

        // then
        Asserts.assertEqualsType(e, IllegalArgumentException.class);
        Asserts.assertEmpty((Map) FieldUtil.getFieldValue(sut, "handlers"));
    }

    private static class InternalHandlers
        extends EzyMQRequestHandlers<EzyTestMQRequestHandler> {}
}
