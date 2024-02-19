package com.hdr.IoTConsumer.Consumer;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import hdrProcessData.common.applicationVersion.ApplicationVersion;
import hdrProcessData.common.protocolVersion.ProtocolVersion;
import hdrProcessData.common.serviceType.ServiceType;
import hdrProcessData.common.version.Version;
import hdrProcessData.services.requisition._4T20._4T20;
import hdrProcessData.services.requisition.health.Health;
import hdrProcessData.services.requisition.rms2.RMS2;
import hdrProcessData.services.requisition.temperature.Temperature;
import hdrProcessData.services.requisition.tilt.Tilt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) throws IOException, TimeoutException {
		Logger logger = LoggerFactory.getLogger(ConsumerApplication.class);

		ConnectionFactory factory = new ConnectionFactory();

		//
		factory.setUsername(RabbitMQConfigs.Username);
		factory.setPassword(RabbitMQConfigs.Password);
		factory.setHost(RabbitMQConfigs.Host);
		factory.setPort(RabbitMQConfigs.Port);
		//

		logger.debug("starting rabbitmq connection...");
		Connection conn = factory.newConnection();
		logger.debug("connected to rabbitmq!");

		logger.debug("creating amqp channel...");
		Channel channel = conn.createChannel();
		logger.debug("amqp channel created!");

		logger.debug("creating rabbitmq topology...");
		channel.exchangeDeclare(RabbitMQConfigs.ExchangeForIoTData, "fanout", true, false, null);
		channel.queueDeclare(RabbitMQConfigs.Queue, false, false, true, null);
		channel.queueBind(RabbitMQConfigs.Queue, RabbitMQConfigs.ExchangeForIoTData, "", null);
		logger.debug("rabbitmq topology created!");

		DeliverCallback deliverCallback = (s, delivery) -> {
			logger.info("received message s:{}", s);

			ObjectMapper mapper = new ObjectMapper();
			IoTData data = mapper.readValue(new String(delivery.getBody(), "UTF-8"), IoTData.class);

			ServiceType sType = ServiceType.getServiceTypeFromInt(data.getServiceType());

			// ProtocolVersion protocol = ProtocolVersion.getProtocolFromString(data.getProtocolVersion());
			ProtocolVersion protocol = ProtocolVersion.V1_1_0;

			// ApplicationVersion application = ApplicationVersion.getApplicationVersionFromString(data.getApplicationVersion());
			ApplicationVersion application = ApplicationVersion.V0;

			Version version = new Version(protocol, application);

			if(sType == ServiceType.health) {
				Health health = new Health().getHealth(data.getRaw(), data.getTime(), version);
			}

			if(sType == ServiceType.temp) {
				Temperature temp = new Temperature().getTemperature(data.getRaw(), data.getTime(), version);
				Double[] temps = temp.getTemp();
				Instant[] times =  temp.getTime();
			}

			if (sType == ServiceType.rms2) {
				RMS2 rms2 = new RMS2().getRMS2(data.getRaw(), data.getTime(), version);
				Double[][] rms2s = rms2.getRms2();
				Instant[] times = rms2.getTime();

				for(Integer i = 0; i <= rms2s.length; i++) {
					Instant t = times[i];
					Double[] values = rms2s[i]; //[0], [1,2,3]
				}
			}

			if (sType == ServiceType.rmms) {}			

			if (sType == ServiceType.tilt) {
				Tilt tilt = new Tilt().getTilt(data.getRaw(), data.getTime(), version);
			}

			if (sType == ServiceType._4t20) {
				_4T20 fort20 = new _4T20().get4T20(data.getRaw(), data.getTime(), version);
				Double[][] corrents = fort20.getCurrent();
				Instant[] times = fort20.getTime();

				for(Integer i = 0; i <= corrents.length; i++) {
					Instant t = times[i];
					Double[] values = corrents[i]; //[0], [1,2]
				}
			}
			if (sType == ServiceType.ntc) {}
			if (sType == ServiceType.pot) {}
		};

		CancelCallback cancelCallback = (s) -> {
			logger.error("error: {}", s);
		};

		channel.basicConsume(RabbitMQConfigs.Queue, true, deliverCallback, cancelCallback);

		SpringApplication.run(ConsumerApplication.class, args);
	}
}

/** 
 * [Sensor] -> [Collectors] <-> [MQTT] -> [bridges] -> [RabbitMQ] -> [filters] -> [RabbitMQ]
 * 
*/

