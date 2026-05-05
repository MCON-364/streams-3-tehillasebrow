package edu.touro.las.mcon364.streams.exercises;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stream practice focused on collecting, grouping, and partitioning.
 *
 * Implement each method using streams.
 * Don't use loops.
 */
public class StreamTaskExercises {

    /**
     * Basics refresher:
     * Return the descriptions of all HIGH priority tasks in encounter order.
     */
    public List<String> highPriorityDescriptions(List<Task> tasks) {
        return tasks.stream()
                .filter(t -> t.priority() == Priority.HIGH)
                .map(Task::description)
                .toList();
    }

    /**
     * Collecting + grouping:
     * Return the number of tasks in each status.
     */
    public Map<Status, Long> countByStatus(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(Task::status, Collectors.counting()));
    }

    /**
     * Grouping + downstream mapping:
     * Group tasks by priority, but keep only task descriptions.
     */
    public Map<Priority, List<String>> descriptionsByPriority(List<Task> tasks) {
        // Fixed: The downstream collector needs to be passed as a second argument to groupingBy.
        // We map each task to its description, then collect those descriptions into a list per priority.
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        Task::priority,
                        Collectors.mapping(Task::description, Collectors.toList())
                ));
    }

    /**
     * Partitioning:
     * Partition tasks into DONE and not DONE.
     * The map keys should be true and false.
     */
    public Map<Boolean, List<Task>> partitionByDone(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.partitioningBy(t -> t.status() == Status.DONE));
    }

    /**
     * Partitioning + downstream counting:
     * Count how many tasks are DONE vs not DONE.
     */
    public Map<Boolean, Long> countDonePartition(List<Task> tasks) {
        // Fixed: The downstream collector goes as the second argument to partitioningBy.
        // This counts tasks in each partition (true for DONE, false for not DONE).
        return tasks.stream()
                .collect(Collectors.partitioningBy(
                        t -> t.status() == Status.DONE,
                        Collectors.counting()
                ));
    }

    /**
     * Nested grouping:
     * First group by status, then by priority.
     */
    public Map<Status, Map<Priority, List<Task>>> groupByStatusThenPriority(List<Task> tasks) {
        return tasks.stream()
                .collect(
                        Collectors.groupingBy(
                                Task::status,
                                Collectors.groupingBy(Task::priority)
                        )
                );
    }

    /**
     * Grouping + mapping + collectingAndThen:
     * Group by status and return alphabetically sorted descriptions for each status.
     */
    public Map<Status, List<String>> sortedDescriptionsByStatus(List<Task> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(
                        Task::status,
                        Collectors.collectingAndThen(
                                Collectors.mapping(Task::description, Collectors.toList()),
                                list -> {
                                    list.sort(String::compareTo);
                                    return list;
                                }
                        )
                ));
    }

    /**
     * Challenge:
     * Return a comma-separated string of descriptions for DONE tasks,
     * preserving encounter order.
     *
     * Example: "Write syllabus, Grade quizzes"
     */
    public String doneTaskSummary(List<Task> tasks) {
        // Filter for DONE tasks, extract their descriptions, and join with ", ".
        // The joining() collector handles the comma separation and maintains encounter order since streams are sequential.
        return tasks.stream()
                .filter(t -> t.status() == Status.DONE)
                .map(Task::description)
                .collect(Collectors.joining(", "));
    }

    /**
     * flatMap:
     * Return all tags from all work items in encounter order.
     */
    public List<String> allTags(List<WorkItem> items) {
        // flatMap is perfect here because each WorkItem has multiple tags (a collection).
        // flatMap flattens all those tag collections into a single stream of individual tags.
        // We use toList() to collect them while preserving encounter order.
        return items.stream()
                .flatMap(item -> item.tags().stream())
                .toList();
    }

    /**
     * flatMap + distinct:
     * Return distinct assignees for DONE items in encounter order.
     */
    public List<String> distinctDoneAssignees(List<WorkItem> items) {
        // Filter to DONE items first, then flatMap their assignees into a single stream.
        // distinct() removes duplicates while preserving encounter order (first occurrence wins).
        // This is more efficient than filtering all items first, then extracting assignees.
        return items.stream()
                .filter(item -> item.status() == Status.DONE)
                .flatMap(item -> item.assignees().stream())
                .distinct()
                .toList();
    }

    /**
     * toMap:
     * Build a map from work-item id to status.
     */
    public Map<String, Status> idToStatus(List<WorkItem> items) {
        // toMap() takes two functions: key mapper and value mapper.
        // WorkItem::id extracts the id (key), and WorkItem::status extracts the status (value).
        // This creates a direct lookup map for quick status retrieval by id.
        return items.stream()
                .collect(Collectors.toMap(WorkItem::id, WorkItem::status));
    }

    /**
     * groupingBy + mapping:
     * Group by priority and collect only titles.
     */
    public Map<Priority, List<String>> titlesByPriorityUsingMapping(List<WorkItem> items) {
        // groupingBy organizes items by priority, and the downstream mapping() collector
        // transforms each WorkItem to just its title before collecting into a list.
        // This is cleaner than manually filtering and mapping after grouping.
        return items.stream()
                .collect(Collectors.groupingBy(
                        WorkItem::priority,
                        Collectors.mapping(WorkItem::title, Collectors.toList())
                ));
    }
}
