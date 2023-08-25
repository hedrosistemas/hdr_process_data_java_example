package com.hdr.IoTConsumer.Consumer;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IoTData {
  private String companyId;
  private String organizationId;
  private String collectorId;
  private Integer messageType;
  private Integer serviceType;
  @JsonProperty("productionSerialNumber")
  private String serialNumber;
  private String protocolVersion;
  private String applicationVersion;
  private String mac;
  private Integer rssi;
  private String raw;
  private Long time;
}
