create type replicator_status as enum ('stopped', 'starting', 'started', 'stopping');

create table
    public.replicators (
        id bigint generated always as identity primary key,
        tenant_id bigint references public.tenants(id) not null,
        status replicator_status not null
    );
