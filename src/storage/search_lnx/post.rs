use serde::{Serialize, Serializer};

fn field_ser<T: Serialize, S: Serializer>(x: T, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    [&x].serialize(s)
}

#[derive(Serialize, Clone, Debug)]
pub struct Post<'a> {
    #[serde(serialize_with = "field_ser")]
    board: &'static str,
    #[serde(serialize_with = "field_ser")]
    thread_no: u64,
    #[serde(serialize_with = "field_ser")]
    post_no: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    subject: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    tripcode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    unique_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    since4_pass: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    image_hash: Option<&'a str>,
    #[serde(serialize_with = "field_ser")]
    image_width: u64,
    #[serde(serialize_with = "field_ser")]
    image_height: u64,
    #[serde(serialize_with = "field_ser")]
    ts: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    comment: Option<String>,
    #[serde(serialize_with = "field_ser")]
    deleted: u64,
    #[serde(serialize_with = "field_ser")]
    ghost: u64,
    #[serde(serialize_with = "field_ser")]
    sticky: u64,
    #[serde(serialize_with = "field_ser")]
    spoiler: u64,
    #[serde(serialize_with = "field_ser")]
    op: u64,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "field_ser")]
    capcode: Option<u64>,
}

impl<'a> From<&'a crate::imageboard::Post> for Post<'a> {
    fn from(post: &'a crate::imageboard::Post) -> Self {
        Post {
            board: post.board,
            thread_no: post.thread_no(),
            post_no: post.no,
            subject: post.sub.as_ref().map(|x| &**x),
            username: post.name.as_ref().map(|x| &**x),
            tripcode: post.trip.as_ref().map(|x| &**x),
            email: post.email.as_ref().map(|x| &**x),
            unique_id: post.id.as_ref().map(|x| &**x),
            since4_pass: post.since4pass.map(|s| s as u64),
            country: post.poster_country(),
            filename: post.media_filename(),
            image_hash: post.md5.as_ref().map(|x| &**x),
            image_width: post.w as _,
            image_height: post.h as _,
            ts: post.time as _,
            comment: post.comment(),
            deleted: if post.deleted { 1 } else { 0 },
            ghost: 0,
            sticky: if post.sticky { 1 } else { 0 },
            spoiler: if post.spoiler { 1 } else { 0 },
            op: if post.is_op() { 1 } else { 0 },
            capcode: post.short_capcode().chars().next().map(|c| c as u64),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DeleteField<T> {
    #[serde(rename = "type")]
    t: &'static str,
    value: [T; 1]
}

#[derive(Debug, Serialize)]
pub struct DeletePost {
    board: DeleteField<&'static str>,
    post_no: DeleteField<u64>,
}


impl From<&crate::imageboard::Post> for DeletePost {
    fn from(post: &crate::imageboard::Post) -> Self {
        DeletePost {
            board: DeleteField {
                t: "text",
                value: [post.board]
            },
            post_no: DeleteField{
                t: "u64",
                value: [post.no]
            },
        }
    }
}


