create schema queue;

create type queue.task_status as enum ('pending', 'in_progress', 'done');

create table
    queue.task_queue (
        id bigserial primary key,
        name text not null,
        data JSONB not null,
        status queue.task_status not null default 'pending',
        created_at timestamptz not null default now (),
        updated_at timestamptz not null default now ()
    );