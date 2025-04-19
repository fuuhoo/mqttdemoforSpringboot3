package sw.fubin.mqttdemoforbootv3;

import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.mqtt.outbound.Mqttv5PahoMessageHandler;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

@SpringBootTest
class MqttdemoForbootv3ApplicationTests {


    @Autowired
    private DefaultListableBeanFactory beanFactory;
    @Test
    void test1() {
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setServerURIs(new String[]{"tcp://192.168.128.114:1883"});
        options.setUserName("swzs");
        options.setPassword("swzs".getBytes());
        options.setCleanStart(false);
        options.setAutomaticReconnect(true);
        //关键问题
        options.setSessionExpiryInterval(1800L);
        String clientId = "test222";
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(Mqttv5PahoMessageHandler.class);
        builder.addConstructorArgValue(options);
        builder.addConstructorArgValue(clientId);
        builder.addPropertyValue("async", false);
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
        beanFactory.registerBeanDefinition("output",beanDefinition );
        Mqttv5PahoMessageHandler messageHandler = beanFactory.getBean("output", Mqttv5PahoMessageHandler.class);

        for (int i = 1; i <= 1000; i++) {
            String msg = "=================================test data indexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=====>" + i;
            String topic = "testRate";
            Message<String> mqttMessage = MessageBuilder.withPayload(msg).setHeader(MqttHeaders.TOPIC, topic)
                    .setHeader(MqttHeaders.QOS, 2).build();
            System.out.println("msg====>"+msg);
            try {

                messageHandler.handleMessage(mqttMessage);
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }

    @Test
    void test2() {
        String broker = "tcp://192.168.128.114:1883";
        String clientId = "test111";
        try {
            org.eclipse.paho.mqttv5.client.MqttClient client = new org.eclipse.paho.mqttv5.client.MqttClient(broker, clientId);
            MqttConnectionOptions options = new MqttConnectionOptions();
            options.setUserName("swzs");
            options.setAutomaticReconnect(true);
            options.setPassword("swzs".getBytes());
            options.setCleanStart(false);
            options.setAutomaticReconnectDelay(0, 120);
            client.connect(options);
            String topic = "testRate";
            for (int i = 1; i <= 1000; i++) {
                String msg = "=================================test data indexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + i;
                org.eclipse.paho.mqttv5.common.MqttMessage message = new org.eclipse.paho.mqttv5.common.MqttMessage(msg.getBytes());
                message.setQos(2);
                System.out.println(msg);
                try {
                    if (!client.isConnected()) {
                        client = new org.eclipse.paho.mqttv5.client.MqttClient(broker, clientId);
                        client.connect(options);
                    }
                    client.publish(topic, message);


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
