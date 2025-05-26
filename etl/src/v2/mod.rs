mod destination;
pub mod pipeline;
mod state;
mod workers;

/*

1. A pipeline is created with a publication_name, a state store where the state of the pipeline is persisted
2. The pipeline will kickstart the apply worker
3.
 */
