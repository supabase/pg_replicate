-- drop foreign keys
alter table app.sources
drop constraint sources_tenant_id_fkey;

alter table app.sinks
drop constraint sinks_tenant_id_fkey;

alter table app.replicators
drop constraint replicators_tenant_id_fkey;

alter table app.pipelines
drop constraint pipelines_tenant_id_fkey;

-- update tables
alter table app.tenants
drop constraint tenants_pkey;

alter table app.tenants
alter column id
drop identity;

alter table app.tenants
alter column id type text;

alter table app.tenants add primary key (id);

alter table app.sources
alter column tenant_id type text;

alter table app.sinks
alter column tenant_id type text;

alter table app.replicators
alter column tenant_id type text;

alter table app.pipelines
alter column tenant_id type text;

-- add foreign keys
alter table app.sources add constraint sources_tenant_id_fkey foreign key (tenant_id) references app.tenants (id);

alter table app.sinks add constraint sinks_tenant_id_fkey foreign key (tenant_id) references app.tenants (id);

alter table app.replicators add constraint replicators_tenant_id_fkey foreign key (tenant_id) references app.tenants (id);

alter table app.pipelines add constraint pipelines_tenant_id_fkey foreign key (tenant_id) references app.tenants (id);