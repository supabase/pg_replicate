create table
    app.sources (
        id bigint generated always as identity primary key,
        tenant_id bigint references app.tenants(id) not null,
        config jsonb not null
    );
