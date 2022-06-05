package com.tvd12.ezymq.activemq.test.setting;

import com.tvd12.ezymq.activemq.setting.EzyActiveTopicSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.Destination;
import javax.jms.Session;

import static org.mockito.Mockito.mock;

public class EzyActiveTopicSettingTest extends BaseTest {

    @Test
    public void test() {
        // given
        Destination topic = mock(Destination.class);
        String topicName = RandomUtil.randomShortAlphabetString();

        int consumerThreadPoolSize = RandomUtil.randomSmallInt() + 1;
        Session session = mock(Session.class);

        // when
        EzyActiveTopicSetting sut = EzyActiveTopicSetting.builder()
            .session(session)
            .topic(topic)
            .topicName(topicName)
            .topicName(topicName)
            .consumerThreadPoolSize(0)
            .consumerThreadPoolSize(consumerThreadPoolSize)
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getTopicName(), topicName);
        Asserts.assertEquals(sut.getConsumerThreadPoolSize(), consumerThreadPoolSize);
    }
}
