create table
    public.pipelines (
        id bigint generated always as identity primary key,
        tenant_id bigint references public.tenants(id) not null,
        source_id bigint references public.sources(id) not null,
        sink_id bigint references public.sinks(id) not null,
        publication_name text not null,
        config jsonb not null
    );
