package com.mylearning.greetingstreams.domain;


import java.time.LocalDateTime;

//@Builder
//@Data
//@AllArgsConstructor
//@NoArgsConstructor
public record Greeting(String message, LocalDateTime timeStamp) {
}


/**
 * import lombok.AllArgsConstructor;
 * import lombok.Builder;
 * import lombok.Data;
 * import lombok.NoArgsConstructor;
 *
 * import java.time.LocalDateTime;
 *
 * @Builder
 * @Data
 * @AllArgsConstructor
 * @NoArgsConstructor
 * public class Greeting {
 *     private String message;
 *     private LocalDateTime timeStamp;
 * }
 */