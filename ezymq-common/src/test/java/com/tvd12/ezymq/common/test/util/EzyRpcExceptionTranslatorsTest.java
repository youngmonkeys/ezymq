package com.tvd12.ezymq.common.test.util;

import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.constant.EzyRpcErrorCodes;
import com.tvd12.ezymq.common.constant.EzyRpcHeaderKeys;
import com.tvd12.ezymq.common.constant.EzyRpcStatusCodes;
import com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.*;

public class EzyRpcExceptionTranslatorsTest extends BaseTest {

    @Test
    public void getExceptionMessageNullTest() {
        // given
        Exception e = new Exception();

        // when
        String actual = getExceptionMessage(e);

        // then
        Asserts.assertEquals(actual, "java.lang.Exception");
    }

    @Test
    public void getExceptionMessageTest() {
        // given
        Exception e = new Exception("test");

        // when
        String actual = getExceptionMessage(e);

        // then
        Asserts.assertEquals(actual, "test");
    }

    @Test
    public void exceptionToResponseHeadersWithNotFoundExceptionTest() {
        // given
        Throwable e = new NotFoundException("test");

        // when
        Map<String, Object> actual = exceptionToResponseHeaders(e);

        // then
        Asserts.assertEquals(
            actual,
            EzyMapBuilder.mapBuilder()
                .put(
                    EzyRpcHeaderKeys.STATUS,
                    EzyRpcStatusCodes.NOT_FOUND
                )
                .put(EzyRpcHeaderKeys.MESSAGE, "test")
                .toMap()
        );
    }

    @Test
    public void exceptionToResponseHeadersWithBadRequestExceptionTest() {
        // given
        Throwable e = new BadRequestException();

        // when
        Map<String, Object> actual = exceptionToResponseHeaders(e);

        // then
        Asserts.assertEquals(
            actual,
            EzyMapBuilder.mapBuilder()
                .put(
                    EzyRpcHeaderKeys.STATUS,
                    EzyRpcStatusCodes.BAD_REQUEST
                )
                .put(
                    EzyRpcHeaderKeys.ERROR_CODE,
                    0
                )
                .put(
                    EzyRpcHeaderKeys.MESSAGE,
                    "com.tvd12.ezyfox.exception.BadRequestException"
                )
                .toMap()
        );
    }

    @Test
    public void exceptionToResponseHeadersWithIllegalArgumentExceptionTest() {
        // given
        Throwable e = new IllegalArgumentException("test");

        // when
        Map<String, Object> actual = exceptionToResponseHeaders(e);

        // then
        Asserts.assertEquals(
            actual,
            EzyMapBuilder.mapBuilder()
                .put(
                    EzyRpcHeaderKeys.STATUS,
                    EzyRpcStatusCodes.BAD_REQUEST
                )
                .put(
                    EzyRpcHeaderKeys.ERROR_CODE,
                    EzyRpcErrorCodes.INVALID_ARGUMENT
                )
                .put(
                    EzyRpcHeaderKeys.MESSAGE,
                    "test"
                )
                .toMap()
        );
    }

    @Test
    public void exceptionToResponseHeadersWithUnsupportedOperationExceptionTest() {
        // given
        Throwable e = new UnsupportedOperationException("test");

        // when
        Map<String, Object> actual = exceptionToResponseHeaders(e);

        // then
        Asserts.assertEquals(
            actual,
            EzyMapBuilder.mapBuilder()
                .put(
                    EzyRpcHeaderKeys.STATUS,
                    EzyRpcStatusCodes.BAD_REQUEST
                )
                .put(
                    EzyRpcHeaderKeys.ERROR_CODE,
                    EzyRpcErrorCodes.UNSUPPORTED_OPERATION
                )
                .put(
                    EzyRpcHeaderKeys.MESSAGE,
                    "test"
                )
                .toMap()
        );
    }

    @Test
    public void exceptionToResponseHeadersWithRuntimeExceptionTest() {
        // given
        Throwable e = new RuntimeException("test");

        // when
        Map<String, Object> actual = exceptionToResponseHeaders(e);

        // then
        Asserts.assertEquals(
            actual,
            EzyMapBuilder.mapBuilder()
                .put(
                    EzyRpcHeaderKeys.STATUS,
                    EzyRpcStatusCodes.INTERNAL_SERVER_ERROR
                )
                .put(
                    EzyRpcHeaderKeys.MESSAGE,
                    "test"
                )
                .toMap()
        );
    }

    @Test
    public void responseHeadersToExceptionWithResponseHeadersIsNull() {
        responseHeadersToException(null);
    }

    @Test
    public void responseHeadersToExceptionWithStatusIsNull() {
        responseHeadersToException(new HashMap<>());
    }

    @Test
    public void responseHeadersToExceptionWithNotFound() {
        // given
        Map<String, Object> responseHeaders = EzyMapBuilder.mapBuilder()
            .put(
                EzyRpcHeaderKeys.STATUS,
                EzyRpcStatusCodes.NOT_FOUND
            )
            .put(EzyRpcHeaderKeys.MESSAGE, "test")
            .toMap();

        // when
        Throwable e = Asserts.assertThrows(() ->
            responseHeadersToException(responseHeaders)
        );

        // then
        Asserts.assertEqualsType(e, NotFoundException.class);
    }

    @Test
    public void responseHeadersToExceptionWithBadRequest() {
        // given
        Map<String, Object> responseHeaders = EzyMapBuilder.mapBuilder()
            .put(
                EzyRpcHeaderKeys.STATUS,
                EzyRpcStatusCodes.BAD_REQUEST
            )
            .put(
                EzyRpcHeaderKeys.ERROR_CODE,
                EzyRpcErrorCodes.INVALID_ARGUMENT
            )
            .put(
                EzyRpcHeaderKeys.MESSAGE,
                "test"
            )
            .toMap();

        // when
        Throwable e = Asserts.assertThrows(() ->
            responseHeadersToException(responseHeaders)
        );

        // then
        Asserts.assertEqualsType(e, BadRequestException.class);
    }

    @Test
    public void responseHeadersToExceptionWithServerError() {
        // given
        Map<String, Object> responseHeaders = EzyMapBuilder.mapBuilder()
            .put(
                EzyRpcHeaderKeys.STATUS,
                EzyRpcStatusCodes.INTERNAL_SERVER_ERROR
            )
            .put(
                EzyRpcHeaderKeys.MESSAGE,
                "test"
            )
            .toMap();

        // when
        Throwable e = Asserts.assertThrows(() ->
            responseHeadersToException(responseHeaders)
        );

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
    }

    @Test
    public void responseHeadersToExceptionWithUnknownStatusError() {
        Map<String, Object> responseHeaders = EzyMapBuilder.mapBuilder()
            .put(EzyRpcHeaderKeys.STATUS, -1)
            .toMap();
        responseHeadersToException(responseHeaders);
    }

    @Override
    public Class<?> getTestClass() {
        return EzyRpcExceptionTranslators.class;
    }
}
