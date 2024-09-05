create schema app;

create table
    app.task_queue (
        id bigint generated always as identity primary key,
        name text not null,
        data JSONB not null
    );
