create table
    public.sources (
        id bigint generated always as identity primary key,
        tenant_id bigint references public.tenants(id),
        config jsonb not null
    );
