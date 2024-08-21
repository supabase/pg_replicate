create table
    public.tenants (
        id bigint generated always as identity primary key,
        supabase_project_ref text,
        prefix text not null,
        name text not null
    );
