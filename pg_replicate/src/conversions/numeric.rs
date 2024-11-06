// adapted from the bigdecimal crate
use bigdecimal::{
    num_bigint::{BigInt, BigUint, Sign},
    BigDecimal,
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{fmt::Display, io::Cursor};
use tokio_postgres::types::{FromSql, Type};

/// A rust variant of the Postgres Numeric type. The full spectrum of Postgres'
/// Numeric value range is supported.
///
/// Represented as an Optional BigDecimal. None for 'NaN', Some(bigdecimal) for
/// all other values.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct PgNumeric {
    pub n: Option<BigDecimal>,
}

impl PgNumeric {
    /// Construct a new PgNumeric value from an optional BigDecimal
    /// (None for NaN values).
    pub fn new(n: Option<BigDecimal>) -> Self {
        Self { n }
    }

    /// Returns true if this PgNumeric value represents a NaN value.
    /// Otherwise returns false.
    pub fn is_nan(&self) -> bool {
        self.n.is_none()
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let n_digits = rdr.read_u16::<BigEndian>()?;
        let weight = rdr.read_i16::<BigEndian>()?;
        let sign = match rdr.read_u16::<BigEndian>()? {
            0x4000 => Sign::Minus,
            0x0000 => Sign::Plus,
            0xC000 => return Ok(Self { n: None }),
            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "").into()),
        };
        let scale = rdr.read_u16::<BigEndian>()?;

        let mut biguint = BigUint::from(0u32);
        for n in (0..n_digits).rev() {
            let digit = rdr.read_u16::<BigEndian>()?;
            biguint += BigUint::from(digit) * BigUint::from(10_000u32).pow(n as u32);
        }

        // First digit in unsigned now has factor 10_000^(digits.len() - 1),
        // but should have 10_000^weight
        //
        // Credits: this logic has been copied from rust Diesel's related code
        // that provides the same translation from Postgres numeric into their
        // related rust type.
        let correction_exp = 4 * (i64::from(weight) - i64::from(n_digits) + 1);
        let res = BigDecimal::new(BigInt::from_biguint(sign, biguint), -correction_exp)
            .with_scale(i64::from(scale));

        Ok(Self { n: Some(res) })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

impl Display for PgNumeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.n {
            Some(ref n) => write!(f, "{n}"),
            None => write!(f, "NaN"),
        }
    }
}
