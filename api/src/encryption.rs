use aws_lc_rs::{
    aead::{Aad, Nonce, RandomizedNonceKey, AES_256_GCM},
    error::Unspecified,
    rand::fill,
};

pub struct EncryptionKey {
    pub id: u32,
    pub key: RandomizedNonceKey,
}

pub fn encrypt(
    plaintext: &[u8],
    key: &RandomizedNonceKey,
) -> Result<(Vec<u8>, Nonce), Unspecified> {
    let mut in_out = plaintext.to_vec();
    let nonce = key.seal_in_place_append_tag(Aad::empty(), &mut in_out)?;
    Ok((in_out, nonce))
}

pub fn decrypt(
    mut ciphertext: Vec<u8>,
    nonce: Nonce,
    key: &RandomizedNonceKey,
) -> Result<Vec<u8>, Unspecified> {
    let plaintext = key.open_in_place(nonce, Aad::empty(), &mut ciphertext)?;
    Ok(plaintext.to_vec())
}

/// Returns a `T` byte long random key encoded as base64
pub fn generate_random_key<const T: usize>() -> Result<RandomizedNonceKey, Unspecified> {
    let mut key_bytes = [0u8; T];
    fill(&mut key_bytes)?;
    let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;
    Ok(key)
}
