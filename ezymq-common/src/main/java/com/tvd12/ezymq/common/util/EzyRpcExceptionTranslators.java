package com.tvd12.ezymq.common.util;

import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezymq.common.constant.EzyRpcErrorCodes;
import com.tvd12.ezymq.common.constant.EzyRpcHeaderKeys;
import com.tvd12.ezymq.common.constant.EzyRpcStatusCodes;

import java.util.HashMap;
import java.util.Map;

import static com.tvd12.ezyfox.io.EzyStrings.EMPTY_STRING;

public final class EzyRpcExceptionTranslators {

    private EzyRpcExceptionTranslators() {}

    public static String getExceptionMessage(Throwable exception) {
        String message = exception.getMessage();
        if (message == null) {
            message = exception.toString();
        }
        return message;
    }

    public static Map<String, Object> exceptionToResponseHeaders(Throwable e) {
        Map<String, Object> responseHeaders = new HashMap<>();
        if (e instanceof NotFoundException) {
            responseHeaders.put(EzyRpcHeaderKeys.STATUS, EzyRpcStatusCodes.NOT_FOUND);
        } else if (e instanceof BadRequestException) {
            BadRequestException badEx = (BadRequestException) e;
            responseHeaders.put(EzyRpcHeaderKeys.STATUS, EzyRpcStatusCodes.BAD_REQUEST);
            responseHeaders.put(EzyRpcHeaderKeys.ERROR_CODE, badEx.getCode());
        } else if (e instanceof IllegalArgumentException) {
            responseHeaders.put(EzyRpcHeaderKeys.STATUS, EzyRpcStatusCodes.BAD_REQUEST);
            responseHeaders.put(EzyRpcHeaderKeys.ERROR_CODE, EzyRpcErrorCodes.INVALID_ARGUMENT);
        } else if (e instanceof UnsupportedOperationException) {
            responseHeaders.put(EzyRpcHeaderKeys.STATUS, EzyRpcStatusCodes.BAD_REQUEST);
            responseHeaders.put(EzyRpcHeaderKeys.ERROR_CODE, EzyRpcErrorCodes.UNSUPPORTED_OPERATION);
        } else {
            responseHeaders.put(EzyRpcHeaderKeys.STATUS, EzyRpcStatusCodes.INTERNAL_SERVER_ERROR);
        }
        String errorMessage = e.getMessage();
        if (errorMessage == null) {
            errorMessage = e.toString();
        }
        responseHeaders.put(EzyRpcHeaderKeys.MESSAGE, errorMessage);
        return responseHeaders;
    }

    public static void responseHeadersToException(
        Map<String, Object> responseHeaders
    ) {
        if (responseHeaders == null) {
            return;
        }
        Integer status = (Integer) responseHeaders.get(EzyRpcHeaderKeys.STATUS);
        if (status == null) {
            return;
        }
        String message = responseHeaders.getOrDefault(
            EzyRpcHeaderKeys.MESSAGE,
            EMPTY_STRING
        ).toString();
        Integer code = (Integer) responseHeaders.get(EzyRpcHeaderKeys.ERROR_CODE);
        if (status.equals(EzyRpcStatusCodes.NOT_FOUND)) {
            throw new NotFoundException(message);
        }
        if (status.equals(EzyRpcStatusCodes.BAD_REQUEST)) {
            throw new BadRequestException(code, message);
        }
        if (status.equals(EzyRpcStatusCodes.INTERNAL_SERVER_ERROR)) {
            throw new InternalServerErrorException(message);
        }
    }
}
