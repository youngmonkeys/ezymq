package com.tvd12.ezymq.mosquitto.test.setting;

import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoEndpointSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyMosquittoEndpointSettingTest extends BaseTest {

    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();

        // when
        EzyMosquittoEndpointSetting sut = new InternalBuilder()
            .topic(topic)
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
    }

    private static class InternalBuilder
        extends EzyMosquittoEndpointSetting.Builder<InternalBuilder> {

        @Override
        public EzyMosquittoEndpointSetting build() {
            return new EzyMosquittoEndpointSetting(
                topic
            );
        }
    }
}
