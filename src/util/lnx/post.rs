use serde::{Serialize, Serializer};

pub fn as_u32_be(array: &[u8; 4]) -> u32 {
    ((array[0] as u32) << 24) +
    ((array[1] as u32) << 16) +
    ((array[2] as u32) <<  8) +
    ((array[3] as u32) <<  0)
}

fn field_ser<T: Serialize, S: Serializer>(x: T, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    [&x].serialize(s)
}

#[derive(Serialize, Clone, Debug)]
pub struct Post<'a> {
    #[serde(serialize_with = "field_ser")]
    pub board: &'a str,
    #[serde(serialize_with = "field_ser")]
    pub thread_no: u64,
    #[serde(serialize_with = "field_ser")]
    pub post_no: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub subject: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub tripcode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub unique_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub since4_pass: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub country: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub filename: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub image_hash: Option<&'a str>,
    #[serde(serialize_with = "field_ser")]
    pub image_width: u64,
    #[serde(serialize_with = "field_ser")]
    pub image_height: u64,
    #[serde(serialize_with = "field_ser")]
    pub ts: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub comment: Option<&'a str>,
    #[serde(serialize_with = "field_ser")]
    pub deleted: u64,
    #[serde(serialize_with = "field_ser")]
    pub ghost: u64,
    #[serde(serialize_with = "field_ser")]
    pub sticky: u64,
    #[serde(serialize_with = "field_ser")]
    pub spoiler: u64,
    #[serde(serialize_with = "field_ser")]
    pub op: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    pub capcode: Option<u64>,
    #[serde(serialize_with = "field_ser")]
    pub version: u64,
    #[serde(serialize_with = "field_ser")]
    pub tuid: u64,
}

#[derive(Debug, Serialize)]
pub struct DeleteField<T> {
    #[serde(rename = "type")]
    t: &'static str,
    value: Vec<T>,
}

#[derive(Debug, Serialize)]
pub struct DeletePost {
    tuid: DeleteField<u64>,
}

impl DeletePost {
    pub fn new(ids: Vec<u64>) -> Self {
        DeletePost {
            tuid: DeleteField {
                t: "u64",
                value: ids,
            }
        }
    }
}