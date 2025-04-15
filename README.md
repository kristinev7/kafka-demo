# Kafka Functionality Showcase Using DataGenerator

This project demonstrates Apache kafka's capabilities using DataGenerator.
The DataGenerator simulates real-world data streaming by generating artificial, but
realistic sensor reading data. 

## Dependencies  
#### *import lombok.Data;*   
- Lombok is a library that reduces Java boilerplate code  
- The @Data annotation automatically generates getters, setters, toString(), equals(), and hashCode() methods for a class  
- Saves you from writing repetitive code for data classes

#### *import com.fasterxml.jackson.core.JsonProcessingException;*  
- Exception thrown when processing JSON content fails
- Used for error handling during JSON serialization/deserialization

#### *import com.fasterxml.jackson.databind.ObjectMapper;*  
- The main Jackson class for converting Java objects to JSON and vice versa  
- Used to serialize your Java objects to JSON strings before sending to Kafka

#### *import lombok.RequiredArgsConstructor;*  
- Lombok annotation that generates a constructor with required arguments for all final fields
- Useful for constructor dependency injection in Spring

#### *import lombok.extern.slf4j.Slf4j;*  
- Lombok annotation that creates a logger instance in your class
- Adds a log field you can use for logging without manual setup

#### *import org.springframework.scheduling.annotation.EnableScheduling;*  
- Spring annotation that enables the scheduling capabilities  
- Allows you to use @Scheduled annotations for running methods at fixed intervals

#### *import org.springframework.stereotype.Component;*
- Spring annotation marking a class as a component
- Tells Spring to detect this class during component scanning and add it to the application context

#### *import javax.annotation.PostConstruct;*  
- Annotation specifying a method to run after dependency injection is complete  
- Used to perform initialization logic after a bean's construction

#### *These imports provide functionality for:*
- Object-to-JSON conversion (Jackson)
- Reducing boilerplate code (Lombok)
- Scheduling recurring tasks (Spring)
- Bean lifecycle management (Spring/JavaEE)
