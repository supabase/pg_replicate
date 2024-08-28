create table
    public.publications (
        id bigint generated always as identity primary key,
        tenant_id bigint references public.tenants(id) not null,
        config jsonb not null
    );
