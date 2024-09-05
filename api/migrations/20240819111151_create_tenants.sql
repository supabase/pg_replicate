create table
    app.tenants (
        id bigint generated always as identity primary key,
        name text not null
    );
