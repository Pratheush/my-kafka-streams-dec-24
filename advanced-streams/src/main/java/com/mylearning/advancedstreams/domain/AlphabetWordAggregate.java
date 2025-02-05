package com.mylearning.advancedstreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @param key
 * @param valueList
 * @param runningCount
 *  FIELDS :::::  String key; Set<String> valueList; int runningCount;
 *
 *  Primary Constructor: The record automatically generates a constructor that accepts all fields:
 *  public AlphabetWordAggregate(String key, Set<String> valueList, int runningCount);
 *
 *
 *  Default Constructor: creates an empty instance
 *  This constructor initializes the key to an empty string, valueList to an empty HashSet, and runningCount to 0. It calls the primary constructor using this.
 *  public AlphabetWordAggregate() {
 *     this("", new HashSet<>(), 0);
 * }
 *
 *
 * updateNewEvents method provides functionality to update the state of the object and return a new updated instance.
 * This method updates the AlphabetWordAggregate by adding a new value to the valueList and increments the runningCount. It creates a new instance of AlphabetWordAggregate with the updated values and returns it.
 * Logs the current state and the new record.
 * Increments runningCount by 1.
 * Adds newValue to valueList.
 * Creates a new AlphabetWordAggregate with updated values.
 * Logs the new aggregated state.
 *
 *  public AlphabetWordAggregate updateNewEvents(String key, String newValue) {
 *     log.info("Before the update : {} , valueList :: {}", this, valueList );
 *     log.info("New Record : key : {} , value : {} : ", key, newValue );
 *     var newRunningCount = this.runningCount +1;
 *     valueList.add(newValue);
 *     var aggregated = new AlphabetWordAggregate(key, valueList, newRunningCount);
 *     log.info("aggregated : {}" , aggregated);
 *     return aggregated;
 * }
 *
 *
 * The main method is the entry point of the program. It creates an instance of AlphabetWordAggregate using the default constructor.
 * The main method does not perform additional operations in this code snippet, but it sets up the initial object.
 * public static void main(String[] args) {
 *     var al = new AlphabetWordAggregate();
 * }
 *
 */
@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount) {


    public AlphabetWordAggregate() {
        this("", new HashSet<>(), 0);
        log.info("AlphabetWordAggregate() DefaultConstructor called");
    }


    // everytime the function is invoked we are going to update the runningCount
    public AlphabetWordAggregate updateNewEvents(String key, String newValue){
        log.info("Before the update : {} , valueList :: {}", this, valueList );
        log.info("New Record : key : {} , value : {} : ", key, newValue );
        var newRunningCount = this.runningCount +1;
        valueList.add(newValue);
        var aggregated = new AlphabetWordAggregate(key, valueList, newRunningCount);
        log.info("aggregated : {}" , aggregated);
        return aggregated;
    }


    public static void main(String[] args) {
        log.info("AlphabetWordAggregate main() called");
        var al =new AlphabetWordAggregate();
    }

}
