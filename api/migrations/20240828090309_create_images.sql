create table
    public.images (
        id bigint generated always as identity primary key,
        image_name text not null,
        is_default boolean not null
    );
