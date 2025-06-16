use etl::v2::conversions::event::Event;
use postgres::schema::Oid;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    Begin,
    Commit,
    Insert,
    Update,
    Delete,
    Relation,
    Truncate,
    Unsupported,
}

impl From<&Event> for EventType {
    fn from(event: &Event) -> Self {
        match event {
            Event::Begin(_) => EventType::Begin,
            Event::Commit(_) => EventType::Commit,
            Event::Insert(_) => EventType::Insert,
            Event::Update(_) => EventType::Update,
            Event::Delete(_) => EventType::Delete,
            Event::Relation(_) => EventType::Relation,
            Event::Truncate(_) => EventType::Truncate,
            &Event::Unsupported => EventType::Unsupported,
        }
    }
}

pub fn group_events_by_type(events: &[Event]) -> HashMap<EventType, Vec<Event>> {
    let mut grouped = HashMap::new();
    for event in events {
        let event_type = EventType::from(event);
        grouped
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(event.clone());
    }

    grouped
}

pub fn group_events_by_type_and_table_id(
    events: &[Event],
) -> HashMap<(EventType, Oid), Vec<Event>> {
    let mut grouped = HashMap::new();
    for event in events {
        let event_type = EventType::from(event);
        // This grouping only works on simple DML operations.
        let table_id = match event {
            Event::Insert(event) => Some(event.table_id),
            Event::Update(event) => Some(event.table_id),
            Event::Delete(event) => Some(event.table_id),
            _ => None,
        };
        if let Some(table_id) = table_id {
            grouped
                .entry((event_type, table_id))
                .or_insert_with(Vec::new)
                .push(event.clone());
        }
    }

    grouped
}

pub fn check_events_count(events: &[Event], conditions: Vec<(EventType, u64)>) -> bool {
    let grouped_events = group_events_by_type(events);
    for (event_type, count) in conditions {
        let Some(inner_events) = grouped_events.get(&event_type) else {
            return false;
        };

        if inner_events.len() != count as usize {
            return false;
        }
    }

    true
}
